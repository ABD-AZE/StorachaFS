import fetch from "node-fetch";
import * as cheerio from "cheerio";

const url = "https://storacha.link/ipfs/bafybeibnpxb5gzg7yagitxsjn5ftta35ttc5xe4k6y5ydrdlnw5hudej3e/";

const res = await fetch(url);
const html = await res.text();

// load into cheerio
const $ = cheerio.load(html);

// find <a> tags (gateway uses links for files)
const files = [];
$("a").each((i, el) => {
  const href = $(el).attr("href");
  if (href && !href.startsWith("?") && href.includes("bafybeibnpxb5gzg7yagitxsjn5ftta35ttc5xe4k6y5ydrdlnw5hudej3e/") && !href.includes("?")) { // skip query links like "?filename"
    files.push(href);
  }
});

console.log("Files:", files);

for (const file of files) {
  const res = await fetch("https://storacha.link" + file);
  const html = await res.text();
  console.log(html);
}