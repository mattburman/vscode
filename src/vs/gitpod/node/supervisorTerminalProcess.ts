/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Typefox. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import { GetTerminalRequest, ListenTerminalRequest, ListenTerminalResponse, OpenTerminalRequest, SetTerminalSizeRequest, ShutdownTerminalRequest, Terminal, TerminalSize, WriteTerminalRequest } from '@gitpod/supervisor-api-grpc/lib/terminal_pb';
import { status } from '@grpc/grpc-js';
import * as util from 'util';
import { Emitter, Event } from 'vs/base/common/event';
import { DisposableStore } from 'vs/base/common/lifecycle';
import * as platform from 'vs/base/common/platform';
import { supervisorDeadlines, supervisorMetadata, terminalServiceClient } from 'vs/gitpod/node/supervisor-client';
import { ILogService } from 'vs/platform/log/common/log';
import { ITerminalChildProcess, ITerminalLaunchError, TerminalShellType } from 'vs/platform/terminal/common/terminal';
import { TerminalDataBufferer } from 'vs/platform/terminal/common/terminalDataBuffering';
import { IPtyHostProcessReplayEvent } from 'vs/platform/terminal/common/terminalProcess';
import { TerminalRecorder } from 'vs/platform/terminal/common/terminalRecorder';

export interface OpenSupervisorTerminalProcessOptions {
	shell: string
	shellArgs: string[]
	env: platform.IProcessEnvironment
	cols: number
	rows: number
}

/**
 * See src/vs/workbench/contrib/terminal/node/terminalProcess.ts for a reference implementation
 */
export class SupervisorTerminalProcess extends DisposableStore implements ITerminalChildProcess {

	private exitCode: number | undefined;
	private closeTimeout: any;
	syncState: Terminal.AsObject | undefined;
	get alias(): string | undefined {
		return this.syncState?.alias;
	}

	private readonly _recorder = new TerminalRecorder(0, 0);

	private readonly _onProcessData = this.add(new Emitter<string>());
	private readonly _onBufferredProcessData = this.add(new Emitter<string>({
		onLastListenerRemove: () => {
			if (!this.shouldPersist) {
				this.logService.info(`code server: ${this.id}:${this.alias}: last client disconnected, shutting down`);
				this.shutdownImmediate();
			}
		}
	}));
	get onProcessData(): Event<string> { return this._onBufferredProcessData.event; }
	private readonly _onProcessExit = this.add(new Emitter<number>());
	get onProcessExit(): Event<number> { return this._onProcessExit.event; }
	private readonly _onProcessReady = this.add(new Emitter<{ pid: number, cwd: string }>());
	get onProcessReady(): Event<{ pid: number, cwd: string }> { return this._onProcessReady.event; }
	private readonly _onProcessReplay = new Emitter<IPtyHostProcessReplayEvent>();
	readonly onProcessReplay = this._onProcessReplay.event;
	private readonly _onProcessTitleChanged = this.add(new Emitter<string>());
	get onProcessTitleChanged(): Event<string> { return this._onProcessTitleChanged.event; }
	private readonly _onProcessShellTypeChanged = this.add(new Emitter<TerminalShellType>());
	get onProcessShellTypeChanged(): Event<TerminalShellType> { return this._onProcessShellTypeChanged.event; }

	private readonly _bufferer: TerminalDataBufferer;

	constructor(
		readonly id: number,
		private initialCwd: string,
		readonly workspaceId: string,
		readonly workspaceName: string,
		readonly shouldPersist: boolean,
		private readonly logService: ILogService,
		private readonly openOptions?: OpenSupervisorTerminalProcessOptions
	) {
		super();
		// Data buffering to reduce the amount of messages going to the renderer
		this._bufferer = new TerminalDataBufferer((_, data) => this._onBufferredProcessData.fire(data));
		this.add(this._bufferer.startBuffering(id, this._onProcessData.event));
		this.add(this.onProcessExit(() => this._bufferer.stopBuffering(this.id)));

		// Data recording for reconnect
		this.add(this._onProcessData.event(e => this._recorder.recordData(e)));
	}

	acknowledgeDataEvent(charCount: number): void {
		// NO-OP for now
	}

	async start(): Promise<ITerminalLaunchError | undefined> {
		if (this.syncState) {
			this._onProcessReady.fire({ pid: this.syncState.pid, cwd: this.syncState.currentWorkdir });
			this._onProcessTitleChanged.fire(this.syncState.title);
			this.triggerReplay();
			this.listen();
			return undefined;
		}
		try {
			if (!this.openOptions) {
				return {
					message: 'launch configuration is missing'
				};
			}
			const request = new OpenTerminalRequest();
			request.setShell(this.openOptions.shell);
			request.setShellArgsList(this.openOptions.shellArgs);
			request.setWorkdir(this.initialCwd);
			for (const name in this.openOptions.env) {
				request.getEnvMap().set(name, this.openOptions.env[name] || '');
			}
			request.getAnnotationsMap().set('workspaceId', this.workspaceId);
			request.getAnnotationsMap().set('workspaceName', this.workspaceName);
			request.getAnnotationsMap().set('shouldPersistTerminal', String(this.shouldPersist));
			request.setSize(this.toSize(this.openOptions.cols, this.openOptions.rows));

			const response = await util.promisify(terminalServiceClient.open.bind(terminalServiceClient, request, supervisorMetadata, {
				deadline: Date.now() + supervisorDeadlines.long
			}))();
			this.syncState = response.getTerminal()!.toObject();
			this.initialCwd = response.getTerminal()!.getCurrentWorkdir() || response.getTerminal()!.getInitialWorkdir();

			this._onProcessReady.fire({ pid: response.getTerminal()!.getPid(), cwd: this.initialCwd });
			const title = response.getTerminal()!.getTitle();
			if (title) {
				// Send initial timeout async to give event listeners a chance to init
				setTimeout(() => {
					this._onProcessTitleChanged.fire(title);
				}, 0);
			}
			this.listen();
			return undefined;
		} catch (err) {
			this.logService.error(`code server: ${this.id} terminal: failed to open:`, err);
			return { message: `A native exception occurred during launch (${err.message})` };
		}
	}

	private listening = false;
	private stopListen: (() => void) | undefined;
	private async listen(): Promise<void> {
		if (this.listening) {
			return;
		}
		this.listening = true;
		if (!this.alias) {
			await new Promise(resolve => {
				if (this['_isDisposed']) {
					return;
				}
				this.add({ dispose: () => resolve(undefined) });
				const readyListener = this.onProcessReady(() => {
					readyListener.dispose();
					resolve(undefined);
				});
				this.add(readyListener);
			});
		}
		if (this['_isDisposed'] || !this.alias) {
			return;
		}
		const alias = this.alias;
		this.add({
			dispose: () => {
				const { stopListen } = this;
				if (stopListen) {
					// see https://github.com/grpc/grpc-node/issues/1652#issuecomment-749237943
					setImmediate(stopListen);
				}
				this.stopListen = undefined;
			}
		});
		while (true) {
			let notFound = false;
			let exitCode: number | undefined;
			try {
				await new Promise((resolve, reject) => {
					if (this['_isDisposed']) {
						return;
					}
					const request = new ListenTerminalRequest();
					request.setAlias(alias);
					const stream = terminalServiceClient.listen(request, supervisorMetadata);
					this.stopListen = stream.cancel.bind(stream);
					stream.on('end', resolve);
					stream.on('error', reject);
					stream.on('data', (response: ListenTerminalResponse) => {
						if (response.hasTitle()) {
							const title = response.getTitle();
							if (title) {
								this._onProcessTitleChanged.fire(title);
							}
						} else if (response.hasData()) {
							let data = '';
							const buffer = response.getData();
							if (typeof buffer === 'string') {
								data += buffer;
							} else {
								data += Buffer.from(buffer).toString();
							}
							if (data !== '') {
								this.fireProcessData(data);
							}
						} else if (response.hasExitCode()) {
							exitCode = response.getExitCode();
						}
					});
				});
			} catch (e) {
				notFound = 'code' in e && e.code === status.NOT_FOUND;
				if (!this['_isDisposed'] && !notFound && !('code' in e && e.code === status.CANCELLED)) {
					this.logService.error(`code server: ${this.id}:${alias} terminal: listening failed:`, e);
				}
			} finally {
				this.stopListen = undefined;
			}
			if (this['_isDisposed']) {
				return;
			}
			if (notFound) {
				this.shutdownImmediate();
			} else if (typeof exitCode === 'number') {
				this.exitCode = exitCode;
				this.shutdownGracefully();
			}
			await new Promise(resolve => setTimeout(resolve, 2000));
		}
	}

	// Allow any trailing data events to be sent before the exit event is sent.
	// See https://github.com/Tyriar/node-pty/issues/72
	private shutdownGracefully() {
		if (this.closeTimeout) {
			clearTimeout(this.closeTimeout);
		}
		this.closeTimeout = setTimeout(() => this.shutdownImmediate(), 250);
	}

	private async shutdownImmediate(): Promise<void> {
		if (this['_isDisposed'] || !this.alias) {
			return;
		}
		// Attempt to kill the pty, it may have already been killed at this
		// point but we want to make sure
		try {
			const request = new ShutdownTerminalRequest();
			request.setAlias(this.alias);
			await util.promisify(terminalServiceClient.shutdown.bind(terminalServiceClient, request, supervisorMetadata, {
				deadline: Date.now() + supervisorDeadlines.short
			}))();
		} catch (e) {
			if (e && e.code === status.NOT_FOUND) {
				// Swallow, the pty has already been killed
			} else {
				this.logService.error(`code server: ${this.id}:${this.alias} terminal: shutdown failed:`, e);
			}
		}
		this._onProcessExit.fire(this.exitCode || 0);
		this.dispose();
	}

	shutdown(immediate: boolean): void {
		if (immediate) {
			this.shutdownImmediate();
		} else {
			this.shutdownGracefully();
		}
	}

	input(data: string): void {
		this.doInput(data, 'utf8');
	}

	async processBinary(data: string): Promise<void> {
		return this.doInput(data, 'binary');
	}

	protected doInput(data: string, encoding: BufferEncoding): Promise<void> {
		if (this['_isDisposed'] || !this.alias) {
			return Promise.reject();
		}
		const request = new WriteTerminalRequest();
		request.setAlias(this.alias);
		request.setStdin(Buffer.from(data, encoding));
		let resolve: () => void;
		let reject: (reason: any) => void;
		const result = new Promise<void>((res, rej) => {
			reject = rej;
			resolve = res;
		});
		terminalServiceClient.write(request, supervisorMetadata, { deadline: Date.now() + supervisorDeadlines.short }, (e, resp) => {
			if (e && e.code !== status.NOT_FOUND) {
				this.logService.error(`code server: ${this.id}:${this.alias} terminal: write failed:`, e);
				reject(e);
			} else {
				resolve();
			}
		});
		return result;
	}

	resize(cols: number, rows: number): void {
		if (this['_isDisposed'] || !this.alias) {
			return;
		}
		const size = this.toSize(cols, rows);
		if (!size) {
			return;
		}

		// Buffered events should flush when a resize occurs
		this._bufferer.flushBuffer(this.id);

		const request = new SetTerminalSizeRequest();
		request.setAlias(this.alias);
		request.setSize(size);
		request.setForce(true);
		terminalServiceClient.setSize(request, supervisorMetadata, { deadline: Date.now() + supervisorDeadlines.short }, e => {
			if (e && e.code !== status.NOT_FOUND) {
				this.logService.error(`code server: ${this.id}:${this.alias} terminal: resize failed:`, e);
			}
		});
	}

	getInitialCwd(): Promise<string> {
		return Promise.resolve(this.initialCwd);
	}

	async getCwd(): Promise<string> {
		if (this['_isDisposed'] || !this.alias) {
			return this.initialCwd;
		}
		try {
			const request = new GetTerminalRequest();
			request.setAlias(this.alias);
			const terminal = await util.promisify(terminalServiceClient.get.bind(terminalServiceClient, request, supervisorMetadata, { deadline: Date.now() + supervisorDeadlines.short }))();
			return terminal.getCurrentWorkdir();
		} catch {
			return this.initialCwd;
		}
	}

	getLatency(): Promise<number> {
		return Promise.resolve(0);
	}

	private fireProcessData(data: string): void {
		this._onProcessData.fire(data);
		if (this.closeTimeout) {
			clearTimeout(this.closeTimeout);
			this.shutdownGracefully();
		}
	}

	private toSize(cols: number, rows: number): TerminalSize | undefined {
		this._recorder.recordResize(cols, rows);

		if (typeof cols !== 'number' || typeof rows !== 'number' || isNaN(cols) || isNaN(rows)) {
			return undefined;
		}
		const size = new TerminalSize();
		size.setCols(Math.max(cols, 0));
		size.setRows(Math.max(rows, 0));
		return size;
	}

	private triggerReplay(): void {
		const event = this._recorder.generateReplayEvent();
		this._onProcessReplay.fire(event);
	}

}
