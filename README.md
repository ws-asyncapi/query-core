# @ws-asyncapi/query-core

The **framework-agnostic** TanStack Query bridge for
[ws-asyncapi](https://github.com/ws-asyncapi). TanStack Query isn't React-only
(`@tanstack/query-core` powers the React, Solid, Vue, Svelte, and Angular
adapters) — and neither is this. Everything here is plain functions and small
subscribable stores; the framework bindings (`@ws-asyncapi/react` today;
`solid` / `vue` / `svelte` next) are thin wrappers over it.

You usually don't install this directly — pick your framework binding. Reach for
it if you're writing a new binding, or wiring ws-asyncapi into TanStack Query in
a framework that doesn't have a binding yet.

## What's in it

**Query / mutation options** — spread into any `@tanstack/*-query` `useQuery` /
`useMutation`:

```ts
import { requestQueryOptions, mutationOptions, historyQueryOptions } from "@ws-asyncapi/query-core";

useQuery(requestQueryOptions(client, "wsaa:/chat/1", "getRoom", { id: "42" }));
useMutation(mutationOptions(client, "sendMessage"));
useQuery(historyQueryOptions(client, "wsaa:/chat/1", "room:42", 50));
```

**Live cache glue** — append incoming events into a history query's cache entry:

```ts
import { subscribeHistoryLive } from "@ws-asyncapi/query-core";
const off = subscribeHistoryLive(client, queryClient, keyPrefix, "room:42", {
  liveEvent: "message",
  limit: 50,
});
```

**Subscribable stores** — the `{ subscribe, getSnapshot }` contract that React's
`useSyncExternalStore`, Solid's `from`, and Vue all adapt:

```ts
import { presenceStore, streamStore, lastEventStore, connectionStore } from "@ws-asyncapi/query-core";

const presence = presenceStore(client);           // { members, self } + set/clear
const ticks = streamStore(client, "prices", input);            // latest value
const ticks2 = streamStore(client, "prices", input, { reduce: "append", max: 100 });
const status = lastEventStore(client, "status");
const conn = connectionStore(client);             // { connected, recovered }
```

**`streamFold`** — the pure latest / `"append"` / custom-`reduce` fold used by
`streamStore` (default keeps only the latest item, O(1)).

## Writing a binding

A binding maps these to its framework's primitives: option factories → that
framework's `useQuery`/`useMutation`; stores → its external-store hook
(`useSyncExternalStore`, `from`, …). See `@ws-asyncapi/react` for the reference.

## Peers

`@tanstack/query-core`, `@ws-asyncapi/client`, `ws-asyncapi`.

## License

MIT
