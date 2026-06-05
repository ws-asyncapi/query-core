/**
 * Framework-agnostic TanStack Query bridge for ws-asyncapi.
 *
 * This package holds everything that is *not* React-specific: query/mutation
 * option factories (consumed by any `@tanstack/*-query` adapter), the
 * live-event → cache glue, the stream fold, and small subscribable stores for
 * presence/streams/events/connection (the `subscribe`/`getSnapshot` contract
 * that React's `useSyncExternalStore`, Solid's `from`, and Vue all adapt).
 *
 * The framework bindings — `@ws-asyncapi/react` today, `@ws-asyncapi/solid` /
 * `vue` / `svelte` later — are thin wrappers over this. TanStack Query itself is
 * not React-locked, and neither is this.
 */
import type { QueryClient } from "@tanstack/query-core";
import { RpcError } from "@ws-asyncapi/client";

/** The subset of a ws-asyncapi client these helpers use (structurally satisfied
 *  by `WsClient`). Kept loose here; the bindings layer on the precise types. */
export interface QueryCoreClient {
    request(
        command: string,
        input: unknown,
        options?: unknown,
    ): Promise<unknown>;
    history(
        room: string,
        options?: { limit?: number },
    ): Promise<Array<{ event: string; data: unknown }>>;
    stream(name: string, input: unknown): AsyncIterable<unknown>;
    onEvent(event: string, cb: (data: unknown) => void): () => void;
    onOpen(cb: () => void): () => void;
    onClose(cb: () => void): () => void;
    onRecover(cb: (recovered: boolean) => void): () => void;
    readonly connected: boolean;
    readonly recovered: boolean;
    presence: {
        get(): Map<string, unknown>;
        readonly self: string | null;
        set(state: unknown): Promise<void>;
        clear(): Promise<void>;
        subscribe(cb: (members: Map<string, unknown>) => void): () => void;
    };
}

/** The external-store contract every framework binding adapts (React's
 *  `useSyncExternalStore`, Solid `from`, Vue `ref`, …). */
export interface Subscribable<T> {
    subscribe(onChange: () => void): () => void;
    getSnapshot(): T;
}

// --- query keys --------------------------------------------------------------

export function rpcQueryKey(
    keyPrefix: string,
    command: string,
    input: unknown,
): unknown[] {
    return [keyPrefix, "rpc", command, input];
}

export function historyQueryKey(
    keyPrefix: string,
    room: string,
    limit?: number,
): unknown[] {
    return [keyPrefix, "history", room, limit];
}

// --- query / mutation option factories (any @tanstack/*-query adapter) -------

/** Options for an RPC-as-query. Spread into any framework's `useQuery`. */
export function requestQueryOptions(
    client: QueryCoreClient,
    keyPrefix: string,
    command: string,
    input: unknown,
) {
    return {
        queryKey: rpcQueryKey(keyPrefix, command, input),
        queryFn: () => client.request(command, input),
    };
}

/** Options for an RPC-as-mutation. Spread into any framework's `useMutation`. */
export function mutationOptions(client: QueryCoreClient, command: string) {
    return {
        mutationFn: (input: unknown) => client.request(command, input),
    };
}

/** Options for a room-history query. Spread into any framework's `useQuery`. */
export function historyQueryOptions(
    client: QueryCoreClient,
    keyPrefix: string,
    room: string,
    limit?: number,
) {
    return {
        queryKey: historyQueryKey(keyPrefix, room, limit),
        queryFn: () => client.history(room, { limit }),
    };
}

/**
 * Keep a history query live by appending incoming `liveEvent` events into the
 * same cache entry (bounded by `limit`). Returns an unsubscribe. Framework-
 * agnostic: pass the adapter's `QueryClient`.
 */
export function subscribeHistoryLive(
    client: QueryCoreClient,
    queryClient: QueryClient,
    keyPrefix: string,
    room: string,
    options: { liveEvent: string; limit?: number },
): () => void {
    const key = historyQueryKey(keyPrefix, room, options.limit);
    return client.onEvent(options.liveEvent, (data) => {
        queryClient.setQueryData(
            key,
            (old: Array<{ event: string; data: unknown }> = []) => {
                const next = [...old, { event: options.liveEvent, data }];
                return options.limit != null
                    ? next.slice(-options.limit)
                    : next;
            },
        );
    });
}

// --- stream fold (pure) ------------------------------------------------------

/** How a stream's yielded items are reduced into the observed value. */
export type StreamReduce<Item, Acc> =
    | { reduce: "append"; max?: number }
    | { reduce: (acc: Acc, item: Item) => Acc; initial: Acc };

/** Build the `{ initial, step }` fold. Default (no options) keeps only the
 *  **latest** item — O(1), nothing accumulated. */
export function streamFold<Item, Acc>(
    options?: StreamReduce<Item, Acc>,
): { initial: unknown; step: (acc: unknown, item: Item) => unknown } {
    if (!options) return { initial: undefined, step: (_acc, item) => item };
    if (options.reduce === "append") {
        const max = options.max;
        return {
            initial: [] as Item[],
            step: (acc, item) => {
                const next = [...(acc as Item[]), item];
                return max != null && next.length > max
                    ? next.slice(-max)
                    : next;
            },
        };
    }
    const { reduce, initial } = options;
    return { initial, step: (acc, item) => reduce(acc as Acc, item) };
}

// --- subscribable stores -----------------------------------------------------

/** Snapshot of a {@link streamStore}. */
export interface StreamSnapshot<Data> {
    data: Data;
    isDone: boolean;
    error: RpcError | null;
}

/** Snapshot of a {@link presenceStore}. */
export interface PresenceSnapshotState {
    members: Map<string, unknown>;
    self: string | null;
}

/** Live presence roster as a subscribable store (+ `set`/`clear`). The snapshot
 *  reference is stable between changes, so it's safe for `useSyncExternalStore`. */
export function presenceStore(
    client: QueryCoreClient,
): Subscribable<PresenceSnapshotState> & {
    set: (state: unknown) => Promise<void>;
    clear: () => Promise<void>;
} {
    let snapshot: PresenceSnapshotState = {
        members: client.presence.get(),
        self: client.presence.self,
    };
    const listeners = new Set<() => void>();
    let unsub: (() => void) | null = null;
    return {
        subscribe(onChange) {
            listeners.add(onChange);
            if (!unsub)
                unsub = client.presence.subscribe((members) => {
                    snapshot = { members, self: client.presence.self };
                    for (const l of listeners) l();
                });
            return () => {
                listeners.delete(onChange);
                if (listeners.size === 0 && unsub) {
                    unsub();
                    unsub = null;
                }
            };
        },
        getSnapshot: () => snapshot,
        set: (state) => client.presence.set(state),
        clear: () => client.presence.clear(),
    };
}

/** Consume a stream as a subscribable store. Default keeps the **latest** value
 *  (O(1)); pass `reduce` to accumulate. Iteration starts on first subscribe and
 *  is cancelled (StreamStop) when the last subscriber leaves. */
export function streamStore(
    client: QueryCoreClient,
    name: string,
    input: unknown,
    options?: StreamReduce<unknown, unknown>,
): Subscribable<StreamSnapshot<unknown>> {
    const fold = streamFold(options);
    let snapshot: StreamSnapshot<unknown> = {
        data: fold.initial,
        isDone: false,
        error: null,
    };
    const listeners = new Set<() => void>();
    let active = false;
    let iter: AsyncIterator<unknown> | null = null;

    const emit = (next: StreamSnapshot<unknown>) => {
        snapshot = next;
        for (const l of listeners) l();
    };

    return {
        subscribe(onChange) {
            listeners.add(onChange);
            if (!active) {
                active = true;
                snapshot = { data: fold.initial, isDone: false, error: null };
                iter = client
                    .stream(name, input)
                    [Symbol.asyncIterator]() as AsyncIterator<unknown>;
                void (async () => {
                    try {
                        while (active) {
                            const { value, done } = await iter.next();
                            if (!active || done) break;
                            emit({
                                ...snapshot,
                                data: fold.step(snapshot.data, value),
                            });
                        }
                        if (active) emit({ ...snapshot, isDone: true });
                    } catch (e) {
                        if (active)
                            emit({
                                ...snapshot,
                                error:
                                    e instanceof RpcError
                                        ? e
                                        : new RpcError("INTERNAL", String(e)),
                            });
                    }
                })();
            }
            return () => {
                listeners.delete(onChange);
                if (listeners.size === 0) {
                    active = false;
                    void iter?.return?.(undefined);
                    iter = null;
                }
            };
        },
        getSnapshot: () => snapshot,
    };
}

/** The most recent value of an event as a subscribable store. */
export function lastEventStore(
    client: QueryCoreClient,
    event: string,
): Subscribable<unknown> {
    let snapshot: unknown;
    const listeners = new Set<() => void>();
    let unsub: (() => void) | null = null;
    return {
        subscribe(onChange) {
            listeners.add(onChange);
            if (!unsub)
                unsub = client.onEvent(event, (data) => {
                    snapshot = data;
                    for (const l of listeners) l();
                });
            return () => {
                listeners.delete(onChange);
                if (listeners.size === 0 && unsub) {
                    unsub();
                    unsub = null;
                }
            };
        },
        getSnapshot: () => snapshot,
    };
}

/** Connection liveness as a subscribable store. */
export function connectionStore(
    client: QueryCoreClient,
): Subscribable<{ connected: boolean; recovered: boolean }> {
    let snapshot = { connected: client.connected, recovered: client.recovered };
    const listeners = new Set<() => void>();
    const offs: Array<() => void> = [];
    const update = (patch: Partial<typeof snapshot>) => {
        snapshot = { ...snapshot, ...patch };
        for (const l of listeners) l();
    };
    return {
        subscribe(onChange) {
            listeners.add(onChange);
            if (offs.length === 0) {
                offs.push(
                    client.onOpen(() => update({ connected: true })),
                    client.onClose(() => update({ connected: false })),
                    client.onRecover((recovered) =>
                        update({ connected: true, recovered }),
                    ),
                );
            }
            return () => {
                listeners.delete(onChange);
                if (listeners.size === 0) {
                    for (const off of offs) off();
                    offs.length = 0;
                }
            };
        },
        getSnapshot: () => snapshot,
    };
}
