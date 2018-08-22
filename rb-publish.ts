// simple publications for redbox-portal



import { ArgumentParser } from 'argparse';
import { Readable } from 'stream';

const mustache = require('mustache');
const fs = require('fs-extra');
const config = require('config');
const path = require('path');
const winston = require('winston');

import { Redbox, Redbox1, Redbox2 } from 'redboxresearchdata-api';
import { FilesApp, FilesDataSet } from 'uts-provisioner-api';

const DEFAULT_CONSOLE_LOG = 'debug';
const DEFAULT_FILE_LOG = 'info';

const trans = [ 
	new winston.transports.Console({
		colorize: true,
		format: winston.format.simple(),
		level: DEFAULT_CONSOLE_LOG
	}),
	new winston.transports.File({
		format: winston.format.simple(),
		level: DEFAULT_FILE_LOG,
		filename: config.get('logfile')
	})	
];

const logger = winston.createLogger({ transports: trans });

async function publish_dataset(options: Object): Promise<void> {
	const oid = options['oid'];
	const template = options['template'];
	const rb = options['redbox'];

	logger.debug(`Looking up record ${oid}`);
	const record = await rb.getRecord(oid);
	if( ! record ) {
		console.error(`No record found with oid ${oid}`);
		return undefined;
	}

	const datarec = record['dataRecord'];
	if( ! datarec ) {
		throw("data publication doesn't have a dataRecord reference");
	}
	if( ! datarec['oid'] ) {
		throw("dataRecord doesn't have an oid");
	}

	const outdir = path.join(options['output'], oid);

	await fs.ensureDir(outdir);

	const droid = datarec['oid'];

	logger.debug(`Data record is ${droid}`);

	if( options['dump'] ) {
		const outfile = path.join(outdir, oid + '.json');
		logger.debug(`Dumping record metadata to ${outfile}`);
		await fs.writeJSON(outfile, record, {spaces: '\t'});
	}

	const attachments = record['dataLocations'].filter((a) => a['type'] === 'attachment');

	const downloads = [];
	for( var i = 0; i < attachments.length; i++ ) {
		const d = await fetch_attachment(rb, droid, outdir, attachments[i]);
		if( d['error'] ) {
			logger.error(`Couldn't fetch attachment ${droid} ${d['fileId']}: ${d['error']}`);
		}
		downloads.push(d);
	}

	record['downloads'] = downloads;

	const indexpath = path.join(outdir, 'index.html');
	logger.debug(`Writing landing page to ${indexpath}`);
	const t = await fs.readFile(template, 'utf8');
  await fs.writeFile(indexpath, mustache.to_html(t, record));

	if( options['dump'] ) {
		const recordjs = path.join(outdir, 'metadata.json');
		logger.debug(`Dumping record metadata to ${recordjs}`);
		await fs.writeJSON(recordjs, record, {spaces: '\t'});
	}




}

// fetch one attachment, return a hash which goes into the template as 
// the download link

async function fetch_attachment(rb, oid, outdir, a) {
	logger.debug(`fetching attachment: ${a['name']} ${a['fileId']}`);
	const fpath = path.join(outdir, a['name']);
	const ds = await rb.readDatastream(oid, a['fileId']);
	if( ds ) {
		logger.debug("Writing to " + fpath);
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
	return {
		'id': a['fileId'],
		'error': 'Error fetching ' + a['name'] + '(' + a['fileId'] + ')'
	};
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

parser.addArgument(
	[ '-d', '--dump'],
	{
		help: "Dump a copy of the object metadata in the output directory",
		action: "storeTrue"
	}
);	

parser.addArgument(
	[ '-p', '--page'],
	{
		help: "Only regenerate the landing page, don't download attachments",
		action: "storeTrue"
	}
);	


const args = parser.parseArgs();
const rbcf = config.get('redbox');



let redbox : Redbox;

logger.debug(`Connecting to redbox: ${rbcf['baseURL']}`);

if ( rbcf['version'] === 'Redbox1' ) {
	redbox = new Redbox1(rbcf);
} else {
	redbox = new Redbox2(rbcf);
}

const OUTDIR = './output';

publish_dataset({
	redbox: redbox,
	oid: args['record'],
	template: config.get('template'),
	output: OUTDIR,
	dump: args['dump'],
	page: args['page']
}).catch((e) => {
	logger.error("Publication failed: " + e);
});


