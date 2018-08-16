// simple publications for redbox-portal



import { ArgumentParser } from 'argparse';
const fs = require('fs-extra');
const config = require('config');

import { Redbox, Redbox1, Redbox2 } from 'redboxresearchdata-api';
import { FilesApp, FilesDataSet } from 'uts-provisioner-api';

const OID = 'd615d23ca4bd704bf9aa297e8fc91d0c';

async function get_dataset(rb: Redbox, oid: string): Promise<void> {

	const record = await rb.getRecord(oid);

	if( ! record ) {
		console.error(`No record found with oid ${oid}`);
		return undefined;
	}

	const attachments = record['dataLocations'];

	attachments.filter((a) => a['type'] === 'attachment').map((a) => {
		console.log("name " + a['name']);
		console.log("fileID " + a['fileId']);
		console.log("\n");
	})

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

get_dataset(redbox, args['record']);


