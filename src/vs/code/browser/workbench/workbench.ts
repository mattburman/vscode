/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

import { isStandalone } from 'vs/base/browser/browser';
import { Schemas } from 'vs/base/common/network';
import { isEqual } from 'vs/base/common/resources';
import { URI } from 'vs/base/common/uri';
import { localize } from 'vs/nls';
import { isFolderToOpen, isWorkspaceToOpen } from 'vs/platform/windows/common/windows';
import { create, IHomeIndicator, IProductQualityChangeHandler, IWorkbenchConstructionOptions, IWorkspace, IWorkspaceProvider } from 'vs/workbench/workbench.web.api';
import { defaultWebSocketFactory } from 'vs/platform/remote/browser/browserSocketFactory';
import { parseLogLevel } from 'vs/platform/log/common/log';

class WorkspaceProvider implements IWorkspaceProvider {

	static QUERY_PARAM_EMPTY_WINDOW = 'ew';
	static QUERY_PARAM_FOLDER = 'folder';
	static QUERY_PARAM_WORKSPACE = 'workspace';

	static QUERY_PARAM_PAYLOAD = 'payload';

	readonly trusted = true;

	constructor(
		readonly workspace: IWorkspace,
		readonly payload: object
	) { }

	async open(workspace: IWorkspace, options?: { reuse?: boolean, payload?: object }): Promise<boolean> {
		if (options?.reuse && !options.payload && this.isSame(this.workspace, workspace)) {
			return true; // return early if workspace and environment is not changing and we are reusing window
		}

		const targetHref = this.createTargetUrl(workspace, options);
		if (targetHref) {
			if (options?.reuse) {
				window.location.href = targetHref;
				return true;
			} else {
				let result;
				if (isStandalone) {
					result = window.open(targetHref, '_blank', 'toolbar=no'); // ensures to open another 'standalone' window!
				} else {
					result = window.open(targetHref);
				}

				return !!result;
			}
		}
		return false;
	}

	private createTargetUrl(workspace: IWorkspace, options?: { reuse?: boolean, payload?: object }): string | undefined {

		// Empty
		let targetHref: string | undefined = undefined;
		if (!workspace) {
			targetHref = `${document.location.origin}${document.location.pathname}?${WorkspaceProvider.QUERY_PARAM_EMPTY_WINDOW}=true`;
		}

		// Folder
		else if (isFolderToOpen(workspace)) {
			targetHref = `${document.location.origin}${document.location.pathname}?${WorkspaceProvider.QUERY_PARAM_FOLDER}=${encodeURIComponent(workspace.folderUri.toString())}`;
		}

		// Workspace
		else if (isWorkspaceToOpen(workspace)) {
			targetHref = `${document.location.origin}${document.location.pathname}?${WorkspaceProvider.QUERY_PARAM_WORKSPACE}=${encodeURIComponent(workspace.workspaceUri.toString())}`;
		}

		// Append payload if any
		if (options?.payload) {
			targetHref += `&${WorkspaceProvider.QUERY_PARAM_PAYLOAD}=${encodeURIComponent(JSON.stringify(options.payload))}`;
		}

		return targetHref;
	}

	private isSame(workspaceA: IWorkspace, workspaceB: IWorkspace): boolean {
		if (!workspaceA || !workspaceB) {
			return workspaceA === workspaceB; // both empty
		}

		if (isFolderToOpen(workspaceA) && isFolderToOpen(workspaceB)) {
			return isEqual(workspaceA.folderUri, workspaceB.folderUri); // same workspace
		}

		if (isWorkspaceToOpen(workspaceA) && isWorkspaceToOpen(workspaceB)) {
			return isEqual(workspaceA.workspaceUri, workspaceB.workspaceUri); // same workspace
		}

		return false;
	}

	hasRemote(): boolean {
		if (this.workspace) {
			if (isFolderToOpen(this.workspace)) {
				return this.workspace.folderUri.scheme === Schemas.vscodeRemote;
			}

			if (isWorkspaceToOpen(this.workspace)) {
				return this.workspace.workspaceUri.scheme === Schemas.vscodeRemote;
			}
		}

		return true;
	}
}

(function () {

	const remoteAuthority = window.location.host + ':443';
	const remoteUserDataElement = document.getElementById('vscode-remote-user-data-uri');
	if (remoteUserDataElement) {
		remoteUserDataElement.setAttribute('data-settings', JSON.stringify({
			scheme: 'vscode-remote',
			authority: remoteAuthority,
			path: '/gitpod-user-data'
		}));
	}

	const webviewEndpoint = new URL(document.baseURI);
	webviewEndpoint.host = 'webview-' + webviewEndpoint.host;
	webviewEndpoint.pathname += 'out/vs/workbench/contrib/webview/browser/pre';
	webviewEndpoint.search = '';
	webviewEndpoint.hash = '';
	const config: IWorkbenchConstructionOptions = {
		remoteAuthority,
		webviewEndpoint: webviewEndpoint.toString()
	};

	// Find workspace to open and payload
	let foundWorkspace = false;
	let workspace: IWorkspace;
	let payload = Object.create(null);
	let logLevel: string | undefined = undefined;

	const query = new URL(document.location.href).searchParams;
	query.forEach((value, key) => {
		switch (key) {

			// Folder
			case WorkspaceProvider.QUERY_PARAM_FOLDER:
				workspace = { folderUri: URI.parse(value) };
				foundWorkspace = true;
				break;

			// Workspace
			case WorkspaceProvider.QUERY_PARAM_WORKSPACE:
				workspace = { workspaceUri: URI.parse(value) };
				foundWorkspace = true;
				break;

			// Empty
			case WorkspaceProvider.QUERY_PARAM_EMPTY_WINDOW:
				workspace = undefined;
				foundWorkspace = true;
				break;

			// Payload
			case WorkspaceProvider.QUERY_PARAM_PAYLOAD:
				try {
					payload = JSON.parse(value);
				} catch (error) {
					console.error(error); // possible invalid JSON
				}
				break;

			// Log level
			case 'logLevel':
				logLevel = value;
				break;
		}
	});

	if (!foundWorkspace) {
		workspace = {
			folderUri: URI.from({
				scheme: 'vscode-remote',
				authority: remoteAuthority,
				path: '/gitpod-folder'
			})
		};
	}

	// Workspace Provider
	const workspaceProvider = new WorkspaceProvider(workspace, payload);

	const homeIndicator: IHomeIndicator = {
		href: 'https://gitpod.io',
		icon: 'code',
		title: localize('home', "Home")
	};


	// Product Quality Change Handler
	const productQualityChangeHandler: IProductQualityChangeHandler = (quality) => {
		let queryString = `quality=${quality}`;

		// Save all other query params we might have
		const query = new URL(document.location.href).searchParams;
		query.forEach((value, key) => {
			if (key !== 'quality') {
				queryString += `&${key}=${value}`;
			}
		});

		window.location.href = `${window.location.origin}?${queryString}`;
	};

	// Finally create workbench
	create(document.body, {
		...config,
		developmentOptions: {
			logLevel: logLevel ? parseLogLevel(logLevel) : undefined,
			...config.developmentOptions
		},
		homeIndicator,
		webSocketFactory: {
			create: url => {
				if (location.protocol.startsWith('https') && url.startsWith('ws:')) {
					return defaultWebSocketFactory.create('wss:' + url.substr('ws:'.length));
				}
				return defaultWebSocketFactory.create(url);
			}
		},
		productQualityChangeHandler,
		workspaceProvider
	});
})();
