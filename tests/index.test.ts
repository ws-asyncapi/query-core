import { describe, expect, it } from "bun:test";
import { QueryClient } from "@tanstack/query-core";
import {
	connectionStore,
	historyQueryKey,
	historyQueryOptions,
	lastEventStore,
	mutationOptions,
	presenceStore,
	type QueryCoreClient,
	requestQueryOptions,
	rpcQueryKey,
	streamFold,
	streamStore,
	subscribeHistoryLive,
} from "../src/index.ts";

/** A controllable fake of the bits these helpers use. */
function fakeClient(overrides: Partial<QueryCoreClient> = {}): QueryCoreClient & {
	emitEvent: (event: string, data: unknown) => void;
	emitPresence: (members: Map<string, unknown>) => void;
	fireOpen: () => void;
	fireClose: () => void;
	fireRecover: (recovered: boolean) => void;
} {
	const eventCbs = new Map<string, Set<(d: unknown) => void>>();
	const presenceCbs = new Set<(m: Map<string, unknown>) => void>();
	const openCbs = new Set<() => void>();
	const closeCbs = new Set<() => void>();
	const recoverCbs = new Set<(r: boolean) => void>();
	let roster = new Map<string, unknown>();

	const base: QueryCoreClient = {
		request: async (command, input) => ({ command, input }),
		history: async () => [],
		stream: async function* () {},
		onEvent: (event, cb) => {
			let set = eventCbs.get(event);
			if (!set) eventCbs.set(event, (set = new Set()));
			set.add(cb);
			return () => set?.delete(cb);
		},
		onOpen: (cb) => {
			openCbs.add(cb);
			return () => openCbs.delete(cb);
		},
		onClose: (cb) => {
			closeCbs.add(cb);
			return () => closeCbs.delete(cb);
		},
		onRecover: (cb) => {
			recoverCbs.add(cb);
			return () => recoverCbs.delete(cb);
		},
		connected: false,
		recovered: false,
		presence: {
			get: () => roster,
			self: "me",
			set: async () => {},
			update: () => {},
			clear: async () => {},
			subscribe: (cb) => {
				presenceCbs.add(cb);
				return () => presenceCbs.delete(cb);
			},
		},
		...overrides,
	};

	return Object.assign(base, {
		emitEvent: (event: string, data: unknown) => {
			for (const cb of eventCbs.get(event) ?? []) cb(data);
		},
		emitPresence: (members: Map<string, unknown>) => {
			roster = members;
			for (const cb of presenceCbs) cb(members);
		},
		fireOpen: () => {
			for (const cb of openCbs) cb();
		},
		fireClose: () => {
			for (const cb of closeCbs) cb();
		},
		fireRecover: (recovered: boolean) => {
			for (const cb of recoverCbs) cb(recovered);
		},
	});
}

describe("query keys", () => {
	it("rpcQueryKey / historyQueryKey are stable, structured tuples", () => {
		expect(rpcQueryKey("chat", "add", { a: 1 })).toEqual([
			"chat",
			"rpc",
			"add",
			{ a: 1 },
		]);
		expect(historyQueryKey("chat", "room:1", 10)).toEqual([
			"chat",
			"history",
			"room:1",
			10,
		]);
	});
});

describe("option factories", () => {
	it("requestQueryOptions wires key + queryFn to client.request", async () => {
		const calls: unknown[] = [];
		const client = fakeClient({
			request: async (c, i) => {
				calls.push([c, i]);
				return { sum: 3 };
			},
		});
		const opts = requestQueryOptions(client, "chat", "add", { a: 1, b: 2 });
		expect(opts.queryKey).toEqual(["chat", "rpc", "add", { a: 1, b: 2 }]);
		expect(await opts.queryFn()).toEqual({ sum: 3 });
		expect(calls).toEqual([["add", { a: 1, b: 2 }]]);
	});

	it("mutationOptions calls client.request with the input", async () => {
		const calls: unknown[] = [];
		const client = fakeClient({
			request: async (c, i) => {
				calls.push([c, i]);
				return "ok";
			},
		});
		const { mutationFn } = mutationOptions(client, "send");
		expect(await mutationFn({ text: "hi" })).toBe("ok");
		expect(calls).toEqual([["send", { text: "hi" }]]);
	});

	it("historyQueryOptions wires key + queryFn to client.history", async () => {
		const calls: unknown[] = [];
		const client = fakeClient({
			history: async (room, o) => {
				calls.push([room, o]);
				return [{ event: "message", data: { text: "x" } }];
			},
		});
		const opts = historyQueryOptions(client, "chat", "room:1", 5);
		expect(opts.queryKey).toEqual(["chat", "history", "room:1", 5]);
		await opts.queryFn();
		expect(calls).toEqual([["room:1", { limit: 5 }]]);
	});
});

describe("subscribeHistoryLive", () => {
	it("appends incoming events into the cache entry, bounded by limit", () => {
		const client = fakeClient();
		const qc = new QueryClient();
		const key = historyQueryKey("chat", "room:1", 2);
		qc.setQueryData(key, [{ event: "message", data: { n: 0 } }]);

		const off = subscribeHistoryLive(client, qc, "chat", "room:1", {
			liveEvent: "message",
			limit: 2,
		});
		client.emitEvent("message", { n: 1 });
		client.emitEvent("message", { n: 2 });

		const cached = qc.getQueryData(key) as Array<{ data: { n: number } }>;
		// limit 2 → oldest dropped
		expect(cached.map((e) => e.data.n)).toEqual([1, 2]);
		off();
		client.emitEvent("message", { n: 3 });
		expect(
			(qc.getQueryData(key) as Array<{ data: { n: number } }>).map(
				(e) => e.data.n,
			),
		).toEqual([1, 2]); // unsubscribed → no further writes
	});
});

describe("streamFold (pure)", () => {
	it("default keeps only the latest item", () => {
		const f = streamFold();
		expect(f.initial).toBeUndefined();
		expect(f.step(1, 2)).toBe(2);
	});

	it("append accumulates and bounds by max", () => {
		const f = streamFold<number, number[]>({ reduce: "append", max: 2 });
		expect(f.initial).toEqual([]);
		let acc = f.step(f.initial, 1);
		acc = f.step(acc, 2);
		acc = f.step(acc, 3);
		expect(acc).toEqual([2, 3]);
	});

	it("custom reduce folds with the provided initial", () => {
		const f = streamFold<number, number>({
			reduce: (acc, item) => acc + item,
			initial: 0,
		});
		expect(f.initial).toBe(0);
		expect(f.step(f.step(0, 2), 3)).toBe(5);
	});
});

describe("presenceStore", () => {
	it("reflects roster changes and delegates set/update/clear", async () => {
		let setArg: unknown;
		const client = fakeClient({
			presence: {
				get: () => new Map(),
				self: "me",
				set: async (s) => {
					setArg = s;
				},
				update: () => {},
				clear: async () => {},
				subscribe: () => () => {},
			},
		});
		const store = presenceStore(client);
		await store.set({ name: "A" });
		expect(setArg).toEqual({ name: "A" });
	});

	it("updates snapshot when the roster changes", () => {
		const client = fakeClient();
		const store = presenceStore(client);
		let notified = 0;
		const off = store.subscribe(() => notified++);
		client.emitPresence(new Map([["x", { name: "X" }]]));
		expect(notified).toBe(1);
		expect(store.getSnapshot().members.get("x")).toEqual({ name: "X" });
		off();
	});
});

describe("streamStore", () => {
	it("folds streamed items (latest) and marks done", async () => {
		async function* gen() {
			yield 1;
			yield 2;
			yield 3;
		}
		const client = fakeClient({ stream: () => gen() });
		const store = streamStore(client, "count", {});
		const off = store.subscribe(() => {});
		// drain microtasks until done
		for (let i = 0; i < 20 && !store.getSnapshot().isDone; i++)
			await Promise.resolve();
		expect(store.getSnapshot().data).toBe(3);
		expect(store.getSnapshot().isDone).toBe(true);
		off();
	});

	it("cancels the iterator when the last subscriber leaves", async () => {
		let returned = false;
		const client = fakeClient({
			stream: () =>
				({
					[Symbol.asyncIterator]() {
						return {
							next: () => new Promise(() => {}), // never resolves
							return: async () => {
								returned = true;
								return { value: undefined, done: true };
							},
						};
					},
				}) as AsyncIterable<unknown>,
		});
		const store = streamStore(client, "count", {});
		const off = store.subscribe(() => {});
		off();
		expect(returned).toBe(true);
	});
});

describe("lastEventStore", () => {
	it("tracks the latest event payload", () => {
		const client = fakeClient();
		const store = lastEventStore(client, "tick");
		const off = store.subscribe(() => {});
		client.emitEvent("tick", { n: 1 });
		client.emitEvent("tick", { n: 2 });
		expect(store.getSnapshot()).toEqual({ n: 2 });
		off();
	});
});

describe("connectionStore", () => {
	it("tracks open/close/recover transitions", () => {
		const client = fakeClient();
		const store = connectionStore(client);
		const off = store.subscribe(() => {});
		client.fireOpen();
		expect(store.getSnapshot().connected).toBe(true);
		client.fireClose();
		expect(store.getSnapshot().connected).toBe(false);
		client.fireRecover(true);
		expect(store.getSnapshot()).toEqual({ connected: true, recovered: true });
		off();
	});
});
