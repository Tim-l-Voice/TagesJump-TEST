const fs = require("fs");
const readline = require("readline");
const { Transform } = require("stream");

async function sortChunks(inputFile, chunkSize) {
  let i = 0;
  let data = [];
  let fileStream = fs.createReadStream(inputFile);
  let rl = readline.createInterface({ input: fileStream });

  for await (let line of rl) {
    data.push(line);
    if (data.length >= chunkSize) {
      data.sort();
      fs.writeFileSync(`chunk${i}.txt`, data.join("\n"));
      i++;
      data = [];
    }
  }

  if (data.length > 0) {
    data.sort();
    fs.writeFileSync(`chunk${i}.txt`, data.join("\n"));
  }
}

async function mergeChunks(chunkFiles, outputFile) {
  let streams = chunkFiles.map((file) =>
    readline.createInterface({ input: fs.createReadStream(file) })
  );
  let queue = [];
  let writeStream = fs.createWriteStream(outputFile);

  for (let i = 0; i < streams.length; i++) {
    let line = await new Promise((resolve) => streams[i].once("line", resolve));
    queue.push({ line, stream: streams[i] });
  }

  while (queue.length > 0) {
    queue.sort((a, b) => a.line.localeCompare(b.line));
    let { line, stream } = queue.shift();
    writeStream.write(line + "\n");
    let nextLine = await new Promise((resolve) => stream.once("line", resolve));
    if (nextLine) queue.push({ line: nextLine, stream });
  }

  writeStream.end();
}

sortChunks("input.txt", 5000000).then(() => {
  let chunkFiles = fs
    .readdirSync(".")
    .filter((file) => file.startsWith("chunk"));
  mergeChunks(chunkFiles, "output.txt");
});
