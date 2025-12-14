import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import Jimp from "jimp";

const s3 = new S3Client({});

async function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

// SQS body contains SNS envelope; SNS "Message" contains the S3 event JSON string.
function extractS3Records(event) {
  const out = [];
  for (const r of event.Records ?? []) {
    const body = JSON.parse(r.body);
    const sns = body.Message ? JSON.parse(body.Message) : body;
    for (const s3rec of sns.Records ?? []) out.push(s3rec);
  }
  return out;
}

export const handler = async (event) => {
  const OUT_BUCKET = process.env.OUT_BUCKET;
  if (!OUT_BUCKET) throw new Error("Missing OUT_BUCKET");

  const records = extractS3Records(event);

  for (const rec of records) {
    const inBucket = rec.s3.bucket.name;
    const inKey = decodeURIComponent(rec.s3.object.key.replace(/\+/g, " "));

    const obj = await s3.send(
      new GetObjectCommand({ Bucket: inBucket, Key: inKey })
    );
    const buf = await streamToBuffer(obj.Body);

    const img = await Jimp.read(buf);
    img.resize(200, Jimp.AUTO);
    const outBuf = await img.getBufferAsync(Jimp.MIME_JPEG);

    const base = inKey.split("/").pop() ?? "image";
    const outKey = `thumbnails/${base}.thumb.jpg`;

    await s3.send(
      new PutObjectCommand({
        Bucket: OUT_BUCKET,
        Key: outKey,
        Body: outBuf,
        ContentType: "image/jpeg",
      })
    );

    console.log(`Wrote thumbnail: s3://${OUT_BUCKET}/${outKey}`);
  }

  return { ok: true, processed: records.length };
};
