const { app } = require("@azure/functions");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

function requireEnv(name) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

const AWS_REGION = process.env.AWS_REGION || "ap-northeast-2";
const AWS_ACCESS_KEY_ID = requireEnv("AWS_ACCESS_KEY_ID");
const AWS_SECRET_ACCESS_KEY = requireEnv("AWS_SECRET_ACCESS_KEY");
const AWS_TARGET_BUCKET = requireEnv("AWS_TARGET_BUCKET");
const S3_TARGET_PREFIX = process.env.S3_TARGET_PREFIX || "from-azure/";

const s3 = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY
  }
});

function normalizeS3KeyPart(name) {
  return String(name || "")
    .replace(/^\/+/, "")
    .replace(/\\/g, "/");
}

app.storageBlob("blobToS3", {
  path: "from-s3/{name}",
  connection: "AzureWebJobsStorage",
  handler: async (blob, context) => {
    const blobName = normalizeS3KeyPart(context.triggerMetadata.name);
    const targetKey = `${S3_TARGET_PREFIX}${blobName}`;

    context.log(`Azure Blob trigger fired. blobName=${blobName}`);
    context.log(`Target S3 bucket=${AWS_TARGET_BUCKET}, key=${targetKey}`);

    if (!blobName) {
      context.log("Skipped. Blob name is empty.");
      return;
    }

    await s3.send(
      new PutObjectCommand({
        Bucket: AWS_TARGET_BUCKET,
        Key: targetKey,
        Body: blob,
        ContentType: "application/octet-stream",
        Metadata: {
          replicatedby: "azure-function",
          source: "azure-blob"
        }
      })
    );

    context.log(`Uploaded to S3 successfully: s3://${AWS_TARGET_BUCKET}/${targetKey}`);
  }
});