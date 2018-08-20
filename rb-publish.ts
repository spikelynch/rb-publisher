// simple publications for redbox-portal



import { ArgumentParser } from 'argparse';
import { Readable } from 'stream';
const mustache = require('mustache');
const fs = require('fs-extra');
const config = require('config');
const path = require('path');

import { Redbox, Redbox1, Redbox2 } from 'redboxresearchdata-api';
import { FilesApp, FilesDataSet } from 'uts-provisioner-api';


async function publish_dataset(options: Object): Promise<void> {
	const oid = options['oid'];
	const template = options['template'];
	const rb = options['redbox'];
	const outdir = options['output'];

	const record = await rb.getRecord(oid);
	if( ! record ) {
		console.error(`No record found with oid ${oid}`);
		return undefined;
	}

	const attachments = record['dataLocations'].filter((a) => a['type'] === 'attachment');
	record['downloads'] = await Promise.all(
		attachments.map((a) => fetch_attachment(rb, oid, outdir, a))
	);

	const indexpath = path.join(outdir, 'index.html');
	const t = await fs.readFile(template, 'utf8');
  await fs.writeFile(indexpath, mustache.to_html(t, record));
}

// fetch one attachment, return a hash which goes into the template as 
// the download link

async function fetch_attachment(rb, oid, outdir, a) {
	console.log(`fetching attachment: ${a['name']} ${a['fileId']}`);
	const fpath = path.join(outdir, a['name']);
	const ds = await rb.readDatastream(oid, a['fileId']);
	if( ds ) {
		console.log("Writing to " + fpath);
		const r = await writefile(ds, fpath);
			if( r ) {
			return {
				'url': a['name'],
				'mimetype': a['mimetype'],
				'size': a['size'],
				'name': a['name']
			} 
		}
	}
	return { 'error': 'Error fetching ' + a['name'] + '(' + a['fileId'] + ')' };
}

async function writefile(stream: Readable, fn: string): Promise<boolean> {
  var wstream = fs.createWriteStream(fn);
  stream.pipe(wstream);
  return new Promise<boolean>( (resolve, reject) => {
    wstream.on('finish', () => { resolve(true) }); 
    wstream.on('error', reject);
  });
}



const parser = new ArgumentParser({
	'version': '1.0.0',
	'addHelp': true,
	'description': "redbox-portal quick publication script"
});

parser.addArgument(
	[ '-r', '--record'],
	{
		help: "OID of redbox-portal record to fetch",
		required: true
	}
);

const args = parser.parseArgs();
const rbcf = config.get('redbox');

let redbox : Redbox;

if ( rbcf['version'] === 'Redbox1' ) {
	redbox = new Redbox1(rbcf);
} else {
	redbox = new Redbox2(rbcf);
}



console.log(`Connecting to redbox at ${rbcf['baseURL']}`);

const OUTDIR = './output';

publish_dataset({
	redbox: redbox,
	oid: args['record'],
	template: config.get('template'),
	output: OUTDIR
});


