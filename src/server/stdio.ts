import process from 'node:process';
import { Readable, Writable } from 'node:stream';
import { JSONRPCMessageValidationError, ReadBuffer, serializeMessage } from '../shared/stdio.js';
import { ErrorCode, JSONRPCMessage, RequestId } from '../types.js';
import { Transport } from '../shared/transport.js';

function getRequestIdFromInvalidMessage(rawMessage: unknown): RequestId | undefined {
    if (rawMessage === null || typeof rawMessage !== 'object' || Array.isArray(rawMessage)) {
        return undefined;
    }

    const id = (rawMessage as { id?: unknown }).id;
    if (typeof id === 'string') {
        return id;
    }
    if (typeof id === 'number' && Number.isInteger(id)) {
        return id;
    }
    return undefined;
}

/**
 * Server transport for stdio: this communicates with an MCP client by reading from the current process' stdin and writing to stdout.
 *
 * This transport is only available in Node.js environments.
 */
export class StdioServerTransport implements Transport {
    private _readBuffer: ReadBuffer;
    private _started = false;

    constructor(
        private _stdin: Readable = process.stdin,
        private _stdout: Writable = process.stdout,
        options?: {
            /**
             * Maximum size of the read buffer in bytes. If a single message exceeds
             * this size the transport will emit an error and close.
             *
             * Defaults to 10 MB.
             */
            maxBufferSize?: number;
        }
    ) {
        this._readBuffer = new ReadBuffer({ maxBufferSize: options?.maxBufferSize });
    }

    onclose?: () => void;
    onerror?: (error: Error) => void;
    onmessage?: (message: JSONRPCMessage) => void;

    // Arrow functions to bind `this` properly, while maintaining function identity.
    _ondata = (chunk: Buffer) => {
        try {
            this._readBuffer.append(chunk);
            this.processReadBuffer();
        } catch (error) {
            this.onerror?.(error as Error);
            this.close().catch(() => {});
        }
    };
    _onerror = (error: Error) => {
        this.onerror?.(error);
    };

    /**
     * Starts listening for messages on stdin.
     */
    async start(): Promise<void> {
        if (this._started) {
            throw new Error(
                'StdioServerTransport already started! If using Server class, note that connect() calls start() automatically.'
            );
        }

        this._started = true;
        this._stdin.on('data', this._ondata);
        this._stdin.on('error', this._onerror);
    }

    private processReadBuffer() {
        while (true) {
            try {
                const message = this._readBuffer.readMessage();
                if (message === null) {
                    break;
                }

                this.onmessage?.(message);
            } catch (error) {
                this.onerror?.(error as Error);
                const id = getRequestIdFromInvalidMessage((error as JSONRPCMessageValidationError).rawMessage);
                if (id !== undefined) {
                    this.send({
                        jsonrpc: '2.0',
                        id,
                        error: {
                            code: ErrorCode.InvalidRequest,
                            message: 'Invalid Request'
                        }
                    }).catch(sendError => {
                        this.onerror?.(sendError);
                    });
                }
            }
        }
    }

    async close(): Promise<void> {
        // Remove our event listeners first
        this._stdin.off('data', this._ondata);
        this._stdin.off('error', this._onerror);

        // Check if we were the only data listener
        const remainingDataListeners = this._stdin.listenerCount('data');
        if (remainingDataListeners === 0) {
            // Only pause stdin if we were the only listener
            // This prevents interfering with other parts of the application that might be using stdin
            this._stdin.pause();
        }

        // Clear the buffer and notify closure
        this._readBuffer.clear();
        this.onclose?.();
    }

    send(message: JSONRPCMessage): Promise<void> {
        return new Promise(resolve => {
            const json = serializeMessage(message);
            if (this._stdout.write(json)) {
                resolve();
            } else {
                this._stdout.once('drain', resolve);
            }
        });
    }
}
