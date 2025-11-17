/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { DurableObject } from 'cloudflare:workers';
import { randomBytes } from 'node:crypto';

const timeout = 12 * 3600;

const base = 'ntfy-cf.tyu.im';

const tutorial = `Bad Request
Follow the tutorial below:

Publish:
    curl -d {content} "https://${base}/{topic}"
Subscription:
    websocat wss://${base}/{topic}/ws
Pull messages:
    curl "https://${base}/{topic}/json"                     # default all
    curl "https://${base}/{topic}/json?since=1763217840"    # since timestamp
    curl "https://${base}/{topic}/json?since=uP4ID5x0fkQh"  # since message id
Auth: # only for ntfy app
    curl "https://${base}/auth" -> \`{ "success": true }\`
Cache and Expires:
    cache messages for 12h and remove every 1h

Docs: https://docs.ntfy.sh/subscribe/api/
Only support partial features.
`;

const topic_regex = /^[A-Za-z0-9\-_]+$/;

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const [_, topic_string] = new URL(request.url).pathname.split('/');
		const topics = topic_string.split(',');
		if (!topics.length || !topics.every((topic) => topic_regex.test(topic))) {
			return new Response(tutorial, { status: 400 });
		}

		const durableObjectId = env.SOCKETS_DURABLE_OBJECT.idFromName('sockets_durable_object');
		const socketsDurableObject: InstanceType<typeof SocketsDurableObject> = env.SOCKETS_DURABLE_OBJECT.get(durableObjectId);

		return socketsDurableObject.fetch(request);
	},
	async scheduled(controller, env, ctx) {
		const durableObjectId = env.SOCKETS_DURABLE_OBJECT.idFromName('sockets_durable_object');
		const socketsDurableObject: InstanceType<typeof SocketsDurableObject> = env.SOCKETS_DURABLE_OBJECT.get(durableObjectId);

		return socketsDurableObject.remove_expired();
	},
} satisfies ExportedHandler<Env>;

export class SocketsDurableObject extends DurableObject<Env> {
	#sockets = new Map<string, Set<WebSocket>>();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	async fetch(request: Request) {
		const params = new URL(request.url).searchParams;
		const [_, topic_string, command] = new URL(request.url).pathname.split('/');
		const topics = topic_string.split(',');
		if (!topics.length || !topics.every((topic) => topic_regex.test(topic))) {
			return new Response(tutorial, { status: 400 });
		}

		switch (true) {
			case request.method === 'GET' && command === 'auth': {
				return new Response(JSON.stringify({ success: true }));
			}
			case request.method === 'GET' && command === 'json': {
				const items = this.#query(topics[0], params.get('since'));

				return new Response(items.map((item) => JSON.stringify({ ...item, event: 'message' })).join('\n') + '\n', {
					headers: {
						'Content-Type': 'application/x-ndjson; charset=utf-8',
						'Access-Control-Allow-Origin': '*',
						'Cache-Control': 'no-cache',
					},
				});
			}
			case (request.method === 'POST' || request.method === 'PUT') && !command: {
				const content = await request.text();
				const response = this.broadcast(topics[0], content || 'triggered');
				return response;
			}
			case request.headers.get('Upgrade') === 'websocket' && command === 'ws': {
				const response = this.connect(topics);
				return response;
			}
		}
		return new Response(tutorial, { status: 400 });
	}

	connect(topics: string[]) {
		const [client, server] = Object.values(new WebSocketPair());
		server.accept();

		topics.forEach((topic) => {
			if (!this.#sockets.get(topic)) this.#sockets.set(topic, new Set());
			this.#sockets.get(topic)!.add(server);
		});

		server.send(
			JSON.stringify({
				id: getRandomId(),
				time: Math.floor(Date.now() / 1000),
				event: 'open',
				topic: topics.join(','),
			}),
		);

		server.addEventListener('close', () => {
			this.close(server);
			server.close();
		});

		server.addEventListener('error', (error) => {
			console.error('WebSocket error:', error);
			this.close(server);
		});

		return new Response(null, {
			status: 101,
			webSocket: client,
		});
	}

	close(server: WebSocket) {
		this.#sockets.forEach((sockets_set, topic, map) => {
			sockets_set.delete(server);
			if (!sockets_set.size) map.delete(topic);
		});
	}

	broadcast(topic: string, content: string) {
		const time = Math.floor(Date.now() / 1000);

		const id = getRandomId();

		const item = {
			id,
			time,
			expires: time + timeout,
			event: 'message',
			topic,
			message: content,
		};
		this.#save(item);

		this.#sockets.get(topic)?.forEach((server) => {
			server.send(JSON.stringify(item));
		});

		return new Response(
			JSON.stringify({
				id,
				time,
				expires: time + timeout,
				event: 'message',
				topic,
				message: content,
			}) + '\n',
		);
	}

	#save({ id, time, expires, topic, message }: Item) {
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS "messages" (
				"id"		TEXT NOT NULL UNIQUE,
				"time"		INTEGER NOT NULL,
				"expires"	INTEGER NOT NULL,
				"topic"		TEXT NOT NULL,
				"message"	TEXT,
				PRIMARY KEY("id","topic")
			);
			INSERT INTO "messages" ("id", "time", "expires", "topic", "message") VALUES
				("${id}", ${time}, ${expires}, "${topic}", "${message}");
			`);
	}

	#query(topic: string, since: string | null = '') {
		const duration_regex = /^(\d+)([smhd])$/;
		switch (true) {
			case !since: {
				return this.ctx.storage.sql
					.exec(
						`
					SELECT "id", "time", "expires", "topic", "message" FROM "messages"
						WHERE "topic" = "${topic}";
					`,
					)
					.toArray() as Item[];
			}
			case since === 'all': {
				// `since` all
				return this.ctx.storage.sql
					.exec(
						`
					SELECT "id", "time", "expires", "topic", "message" FROM "messages"
						WHERE "topic" = "${topic}";
					`,
					)
					.toArray() as Item[];
			}
			case since?.length === 10: {
				// `since` is timestamp(seconds)
				return this.ctx.storage.sql
					.exec(
						`
					SELECT "id", "time", "expires", "topic", "message" FROM "messages"
						WHERE "topic" = "${topic}" AND "time" >= "${since}";
					`,
					)
					.toArray() as Item[];
			}
			case since?.length === 12: {
				// `since` is messgae id
				return this.ctx.storage.sql
					.exec(
						`
					SELECT "id", "time", "expires", "topic", "message" FROM "messages"
						WHERE "topic" = "${topic}"
						AND time >= (SELECT "time" FROM "messages" WHERE "id" = "${since}");
					`,
					)
					.toArray() as Item[];
			}
			case duration_regex.test(since || ''): {
				const [_, num, unit] = since!.match(duration_regex)!;
				let timestamp = Math.floor(Date.now() / 1000);
				switch (unit) {
					case 's':
					case 'S': {
						timestamp -= parseInt(num) * 1;
						break;
					}
					case 'm':
					case 'M': {
						timestamp -= parseInt(num) * 60;
						break;
					}
					case 'h':
					case 'H': {
						timestamp -= parseInt(num) * 3600;
						break;
					}
					case 'd':
					case 'D': {
						timestamp -= parseInt(num) * 3600 * 24;
						break;
					}
				}
				return this.ctx.storage.sql
					.exec(
						`
					SELECT "id", "time", "expires", "topic", "message" FROM "messages"
						WHERE "topic" = "${topic}" AND time >= ${timestamp};
					`,
					)
					.toArray() as Item[];
			}
		}

		return [];
	}

	remove_expired() {
		const now = Math.floor(Date.now() / 1000);
		this.ctx.storage.sql.exec(`
			DELETE FROM "messages" WHERE "expires" < ${now};
			`);
	}
}

const getRandomId = (length = 12) =>
	randomBytes(length)
		.reduce((result, byte) => result + '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'[byte % 62], '')
		.slice(0, length);

type Item = { id: string; time: number; expires: number; topic: string; message: string };
