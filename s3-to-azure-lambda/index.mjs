import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";

const AWS_REGION = process.env.AWS_REGION || "ap-northeast-2";
const S3_SOURCE_PREFIX = process.env.S3_SOURCE_PREFIX || "aws-inbox/";
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const AZURE_TARGET_CONTAINER = process.env.AZURE_TARGET_CONTAINER || "from-s3";

const s3 = new S3Client({ region: AWS_REGION });

function requireEnv(name, value) {
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
}

async function streamToBuffer(stream) {
  const chunks = [];

  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

function decodeS3Key(key) {
  return decodeURIComponent(key.replace(/\+/g, " "));
}

export const handler = async (event) => {
  requireEnv("AZURE_STORAGE_CONNECTION_STRING", AZURE_STORAGE_CONNECTION_STRING);

  console.log("Received S3 event:", JSON.stringify(event, null, 2));

  const blobServiceClient = BlobServiceClient.fromConnectionString(
    AZURE_STORAGE_CONNECTION_STRING
  );

  const containerClient = blobServiceClient.getContainerClient(AZURE_TARGET_CONTAINER);
  await containerClient.createIfNotExists();

  const results = [];

  for (const record of event.Records ?? []) {
    const bucketName = record.s3.bucket.name;
    const rawKey = record.s3.object.key;
    const objectKey = decodeS3Key(rawKey);

    console.log(`Processing bucket=${bucketName}, key=${objectKey}`);

    if (!objectKey.startsWith(S3_SOURCE_PREFIX)) {
      console.log(`Skipped. Key does not start with ${S3_SOURCE_PREFIX}`);
      continue;
    }

    const targetBlobName = objectKey.substring(S3_SOURCE_PREFIX.length);

    if (!targetBlobName) {
      console.log("Skipped. Target blob name is empty.");
      continue;
    }

    const getObjectResponse = await s3.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: objectKey
      })
    );

    const bodyBuffer = await streamToBuffer(getObjectResponse.Body);

    const blockBlobClient = containerClient.getBlockBlobClient(targetBlobName);

    await blockBlobClient.uploadData(bodyBuffer, {
      blobHTTPHeaders: {
        blobContentType: getObjectResponse.ContentType || "application/octet-stream"
      },
      metadata: {
        replicatedby: "aws-lambda",
        source: "s3",
        sourcebucket: bucketName
      }
    });

    console.log(
      `Uploaded to Azure Blob: container=${AZURE_TARGET_CONTAINER}, blob=${targetBlobName}, size=${bodyBuffer.length}`
    );

    results.push({
      sourceBucket: bucketName,
      sourceKey: objectKey,
      targetContainer: AZURE_TARGET_CONTAINER,
      targetBlob: targetBlobName,
      size: bodyBuffer.length
    });
  }

  return {
    statusCode: 200,
    body: JSON.stringify({
      message: "S3 to Azure Blob replication completed",
      results
    })
  };
};