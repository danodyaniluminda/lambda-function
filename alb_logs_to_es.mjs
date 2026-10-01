import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { randomBytes } from 'node:crypto';
import { createHmac } from 'node:crypto';
import { createHash } from 'node:crypto';

// Configuration is provided via Lambda environment variables (see Terraform:
// environments/non-production/dev/alb-logs-to-es/locals-lambda.tf).
const endpoint = process.env.ES_ENDPOINT;
const indexName = process.env.ES_INDEX;
const region = process.env.AWS_REGION || 'ap-southeast-1';
const logFailedResponses = (process.env.LOG_FAILED_RESPONSES || 'true') === 'true';

if (!endpoint || !indexName) {
    throw new Error("Missing required environment variables: ES_ENDPOINT and ES_INDEX");
}

const myRegexp = /([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^ ]*)\" \"([^\s]+?)\" \"([^\s]+)\" \"([^ ]*)\" \"([^ ]*)\"/;

// Initialize the S3 client
const s3Client = new S3Client({ region });

export const handler = async (event, context) => {
    const currentDate = new Date();
    const year = currentDate.getFullYear();
    const month = (currentDate.getMonth() + 1).toString().padStart(2, '0'); // Month is zero-based so we add 1
    const day = currentDate.getDate().toString().padStart(2, '0');
    const formattedDate = `${year}-${month}-${day}`;

    try {
        const bucket = event.Records[0].s3.bucket.name;
        const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
        if (!bucket || !key) {
            throw new Error("Bucket name and object key are required");
        }
        const command = new GetObjectCommand({
            Bucket: bucket, // Must be a non-empty string
            Key: key        // Must be a non-empty string
        });
        const response = await s3Client.send(command);

        // Create a readable stream from the S3 object body
        const stream = Readable.from(response.Body);

        // Pipe the stream through gunzip to decompress
        const unzippedStream = stream.pipe(createGunzip());

        // Read the unzipped content
        const chunks = [];
        for await (const chunk of unzippedStream) {
            chunks.push(chunk);
        }
        const logData = Buffer.concat(chunks).toString('utf-8');
        const array = logData.split("\n");
        let bulkRequestBody = '';

        for (const line of array) {
            const bulkRes = transform(line, key, formattedDate);
            if (bulkRes != null) {
                bulkRequestBody += bulkRes;
            }
        }

        // skip control messages
        if (!bulkRequestBody) {
            console.log('Control message handled successfully');
            return 'Control message handled successfully';
        }

        // post documents to the Amazon Elasticsearch / OpenSearch Service
        const { success, failedItems } = await post(bulkRequestBody);
        console.log('Success:', JSON.stringify(success));

        if (failedItems && failedItems.length > 0) {
            logFailure(null, failedItems);
        }

        return {
            statusCode: 200,
            body: JSON.stringify('Success'),
        };
    } catch (error) {
        console.error("Failure:", JSON.stringify(error));
        logFailure(error, error && error.failedItems);
        return {
            statusCode: 500,
            body: JSON.stringify(error)
        };
    }
};

function transform(line, key, formattedDate) {
    const source = {};
    const indeid = randomBytes(20).toString("hex");

    const match = myRegexp.exec(line) || [];
    const [, type, time, elb, client_ip, client_port, target_ip, target_port, request_processing_time, target_processing_time, response_processing_time, elb_status_code, target_status_code, received_bytes, sent_bytes, request_type, request_url, request_protocol, user_agent_browser, ssl_cipher, ssl_protocol, target_group_arn, trace_id, domain_name, chosen_cert_arn, matched_rule_priority, request_creation_time, actions_executed, redirect_url, lambda_error_reason, target_port_list, target_status_code_list, classification, classification_reason] = match;

    if (type == null) {
        return null;
    }

    const trimmedUrl = (request_url || '').replace(/^-+\s*/, '').replace(/-+$/, '');

    // Guard against unparsable URLs so a single bad line doesn't fail the batch
    let url_pathname = '-';
    let url = [];
    try {
        url_pathname = new URL(trimmedUrl).pathname;
        url = url_pathname.split("/");
    } catch (e) {
        if (logFailedResponses) {
            console.log('Skipping URL parse for line, invalid url:', trimmedUrl);
        }
    }

    source['@id'] = indeid;
    source['@type'] = type;
    source['@time'] = time || new Date().toISOString();
    source['@elb'] = elb || '-';
    source['@client_ip'] = client_ip || '-';
    source['@client_port'] = client_port || '-';
    source['@target_ip'] = target_ip || '-';
    source['@target_port'] = target_port || '-';
    source['@request_processing_time'] = request_processing_time || '-';
    source['@target_processing_time'] = target_processing_time || '-';
    source['@response_processing_time'] = response_processing_time || '-';
    source['@elb_status_code'] = elb_status_code || '-';
    source['@target_status_code'] = target_status_code || '-';
    source['@received_bytes'] = received_bytes || '-';
    source['@sent_bytes'] = sent_bytes || '-';
    source['@request_type'] = request_type || '-';
    source['@request_url'] = trimmedUrl || '-';
    source['@request_protocol'] = request_protocol || '-';
    source['@user_agent_browser'] = user_agent_browser || '-';
    source['@ssl_cipher'] = ssl_cipher || '-';
    source['@ssl_protocol'] = ssl_protocol || '-';
    source['@target_group_arn'] = target_group_arn || '-';
    source['@trace_id'] = trace_id || '-';
    source['@domain_name'] = domain_name || '-';
    source['@chosen_cert_arn'] = chosen_cert_arn || '-';
    source['@matched_rule_priority'] = matched_rule_priority || '-';
    source['@request_creation_time'] = request_creation_time || new Date().toISOString();
    source['@actions_executed'] = actions_executed || '-';
    source['@redirect_url'] = redirect_url || '-';
    source['@lambda_error_reason'] = lambda_error_reason || '-';
    source['@target_port_list'] = target_port_list || '-';
    source['@target_status_code_list'] = target_status_code_list || '-';
    source['@classification'] = classification || '-';
    source['@classification_reason'] = classification_reason || '-';
    source['@message'] = line || '-';
    source['@s3_key'] = key || '-';
    source['@pathname'] = url_pathname || '-';
    source['@context_path'] = url[1] || '-';
    source['@path_1'] = url[2] || '-';
    source['@path_2'] = url[3] || '-';
    source['@path_3'] = url[4] || '-';
    source['@path_4'] = url[5] || '-';
    source['@timestamp'] = new Date().toISOString();
    source['@app_path'] = [(domain_name || '-'), (url[1] || '-')].join();

    const action = { "index": {} };
    action.index._index = `${indexName}_${formattedDate}`;
    action.index._id = indeid;

    return [
        JSON.stringify(action),
        JSON.stringify(source),
    ].join('\n') + '\n';
}

async function post(body) {
    const requestParams = buildRequest(endpoint, body);

    const response = await fetch(`https://${requestParams.host}${requestParams.path}`, {
        method: requestParams.method,
        headers: requestParams.headers,
        body: requestParams.body,
    });

    const info = await response.json();
    let failedItems = [];
    let success;

    if (response.ok) {
        failedItems = info.items.filter(item => item.index.status >= 300);
        success = {
            attemptedItems: info.items.length,
            successfulItems: info.items.length - failedItems.length,
            failedItems: failedItems.length,
        };
    }

    if (!response.ok || info.errors === true) {
        delete info.items;
        const error = {
            statusCode: response.status,
            responseBody: info,
            failedItems,
        };
        throw error;
    }

    return { success, failedItems };
}

function buildRequest(endpoint, body) {
    const endpointParts = endpoint.match(/^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/);
    const esRegion = endpointParts[2];
    const service = endpointParts[3];
    const datetime = (new Date()).toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const date = datetime.substr(0, 8);
    const kDate = hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
    const kRegion = hmac(kDate, esRegion);
    const kService = hmac(kRegion, service);
    const kSigning = hmac(kService, 'aws4_request');

    const request = {
        host: endpoint,
        method: 'POST',
        path: '/_bulk',
        body: body,
        headers: {
            'Content-Type': 'application/json',
            'Host': endpoint,
            'Content-Length': Buffer.byteLength(body),
            'X-Amz-Security-Token': process.env.AWS_SESSION_TOKEN,
            'X-Amz-Date': datetime
        }
    };

    const canonicalHeaders = Object.keys(request.headers)
        .sort((a, b) => (a.toLowerCase() < b.toLowerCase() ? -1 : 1))
        .map(k => k.toLowerCase() + ':' + request.headers[k])
        .join('\n');

    const signedHeaders = Object.keys(request.headers)
        .map(k => k.toLowerCase())
        .sort()
        .join(';');

    const canonicalString = [
        request.method,
        request.path, '',
        canonicalHeaders, '',
        signedHeaders,
        hash(request.body, 'hex'),
    ].join('\n');

    const credentialString = [date, esRegion, service, 'aws4_request'].join('/');

    const stringToSign = [
        'AWS4-HMAC-SHA256',
        datetime,
        credentialString,
        hash(canonicalString, 'hex')
    ].join('\n');

    request.headers.Authorization = [
        'AWS4-HMAC-SHA256 Credential=' + process.env.AWS_ACCESS_KEY_ID + '/' + credentialString,
        'SignedHeaders=' + signedHeaders,
        'Signature=' + hmac(kSigning, stringToSign, 'hex')
    ].join(', ');

    return request;
}

function hmac(key, str, encoding) {
    return createHmac('sha256', key).update(str, 'utf8').digest(encoding);
}

function hash(str, encoding) {
    return createHash('sha256').update(str, 'utf8').digest(encoding);
}

function logFailure(error, failedItems) {
    if (logFailedResponses) {
        if (error) {
            console.log('Error: ' + JSON.stringify(error, null, 2));
        }
        if (failedItems && failedItems.length > 0) {
            console.log("Failed Items: " + JSON.stringify(failedItems, null, 2));
        }
    }
}
