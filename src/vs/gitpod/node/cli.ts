/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Typefox. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import * as path from 'path';
import { URI } from 'vs/base/common/uri';
import * as assert from 'assert';
import { ErrorReporter, parseArgs, OptionDescriptions, OPTIONS as ALL_OPTIONS, buildHelpMessage, buildVersionMessage } from 'vs/platform/environment/node/argv';
import { localize } from 'vs/nls';
import product from 'vs/platform/product/common/product';
import { streamToBufferReadableStream } from 'vs/base/common/buffer';
import { IRequestContext } from 'vs/base/parts/request/common/request';
import { asJson } from 'vs/platform/request/common/request';
import type { OpenCommandPipeArgs } from 'vs/workbench/api/node/extHostCLIServer';
import { createWaitMarkerFile } from 'vs/platform/environment/node/waitMarkerFile';
import { whenDeleted, lstat } from 'vs/base/node/pfs';
import { NativeParsedArgs } from 'vs/platform/environment/common/argv';

const OPTIONS_KEYS: (keyof typeof ALL_OPTIONS)[] = [
	'help',
	'version',
	'verbose',

	'diff',
	'add',
	'goto',
	'new-window',
	'reuse-window',
	'folder-uri',
	'file-uri',
	'wait'
];
const OPTIONS: OptionDescriptions<NativeParsedArgs> = {
	_: ALL_OPTIONS['_']
};
for (const key of OPTIONS_KEYS) {
	Object.assign(OPTIONS, { [key]: ALL_OPTIONS[key] });
}

const devMode = !!process.env['VSCODE_DEV'];

async function main(processArgv: string[]): Promise<any> {
	let args: NativeParsedArgs;

	try {
		const errorReporter: ErrorReporter = {
			onUnknownOption: (id) => {
				console.warn(localize('unknownOption', "Warning: '{0}' is not in the list of known options.", id));
			},
			onMultipleValues: (id, val) => {
				console.warn(localize('multipleValues', "Option '{0}' is defined more than once. Using value '{1}.'", id, val));
			}
		};

		args = parseArgs(processArgv.slice(2), OPTIONS, errorReporter);
		if (args.goto) {
			args._.forEach(arg => assert(/^(\w:)?[^:]+(:\d*){0,2}$/.test(arg), localize('gotoValidation', "Arguments in `--goto` mode should be in the format of `FILE(:LINE(:CHARACTER))`.")));
		}
	} catch (err) {
		console.error(err.message);
		return;
	}

	// Help
	if (args.help) {
		const executable = `${product.applicationName}`;
		console.log(buildHelpMessage(product.nameLong, executable, product.version, OPTIONS));
	}

	// Version Info
	else if (args.version) {
		console.log(buildVersionMessage(product.version, product.commit));
	}

	// Just Code
	else {
		let port = 3000;
		if (!devMode && process.env.GITPOD_THEIA_PORT) {
			port = Number(process.env.GITPOD_THEIA_PORT);
		}
		const http = await import('http');
		interface LinksResult {
			links: string[]
		}
		const result = await new Promise<LinksResult | null>((resolve, reject) => {
			const request = http.request({
				hostname: 'localhost',
				port,
				protocol: 'http:',
				path: '/gitpod-cli-server-sockets',
				method: 'GET',
				timeout: 5000
			}, response => {
				if (response.statusCode !== 200) {
					reject(new Error(`failed to fetch cli server sockets: ${response.statusCode} (${response.statusMessage})`));
					return;
				}
				try {
					resolve(asJson<LinksResult>({
						res: response, stream: streamToBufferReadableStream(response)
					} as IRequestContext));
				} catch (e) {
					reject(e);
				}
			});
			request.on('error', reject);
			request.end();
		});
		if (!result?.links.length) {
			throw new Error('please open window');
		}
		if (result.links.length > 1) {
			throw new Error('there should be only one opened window, please close other');
		}
		const link = result.links[0];
		const waitMarkerFilePath = args.wait ? createWaitMarkerFile(args.verbose) : undefined;
		const fileURIs: string[] = [...args['file-uri'] || []];
		const folderURIs: string[] = [...args['folder-uri'] || []];
		const pendingFiles: Promise<void>[] = [];
		for (const arg of args._) {
			if (arg === '-') {
				// don't support reading from stdin yet
				continue;
			}
			const filePath = path.resolve(process.cwd(), arg);
			pendingFiles.push(lstat(filePath).then(stat => {
				const uris = stat.isFile() ? fileURIs : folderURIs;
				uris.push(URI.parse(filePath).toString());
			}, e => {
				if (e.code === 'ENOENT') {
					// open a new file
					fileURIs.push(URI.parse(filePath).toString());
				} else {
					console.log(`failed to resolve '${filePath}' path:`, e);
				}
			}));
		}
		await Promise.all(pendingFiles);
		const openComamnd: OpenCommandPipeArgs = {
			type: 'open',
			fileURIs,
			folderURIs,
			forceNewWindow: args['new-window'],
			diffMode: args.diff,
			addMode: args.add,
			gotoLineMode: args.goto,
			forceReuseWindow: args['reuse-window'],
			waitMarkerFilePath
		};
		await new Promise((resolve, reject) => {
			const req = http.request({
				socketPath: link,
				method: 'POST',
				timeout: 5000
			}, res => {
				if (res.statusCode !== 200) {
					return reject(new Error(`Bad status code: ${res.statusCode}`));
				}

				const chunks: string[] = [];
				res.setEncoding('utf8');
				res.on('data', d => chunks.push(d));
				res.on('end', () => resolve(chunks.join('')));
			});

			req.on('error', err => reject(err));
			req.write(JSON.stringify(openComamnd));
			req.end();
		});
		if (waitMarkerFilePath) {
			// Complete when wait marker file is deleted
			await whenDeleted(waitMarkerFilePath);
		}
	}
}

function eventuallyExit(code: number): void {
	setTimeout(() => process.exit(code), 0);
}

main(process.argv)
	.then(() => eventuallyExit(0))
	.then(null, err => {
		console.error(err.message || err.stack || err);
		eventuallyExit(1);
	});
