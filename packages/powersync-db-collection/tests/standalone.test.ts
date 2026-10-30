import { createCollection, createLiveQueryCollection, eq } from '@tanstack/db';
import { afterEach, expect, it, vi } from 'vitest';
import { Schema, Table, column } from '@powersync/common';
import { PowerSyncClient, StreamError,  powerSyncCollectionOptions } from '../src';
import type {StreamSubscription} from '../src';

const APP_SCHEMA = new Schema({
  tasks: new Table({ title: column.text, board: column.text }),
  lists: new Table({ title: column.text })
});
type Row = Record<string, unknown>;

/** A fake PowerSync service, serving one NDJSON response per sync request. */
class FakeService {
  readonly requests: Array<Record<string, any>> = [];
  readonly checkpointRequests: Array<Record<string, any>> = [];
  #stream?: ReadableStreamDefaultController<Uint8Array>;
  #signal?: AbortSignal;
  #opId = 0;
  /** Runs before responding to a checkpoint request. */
  beforeCheckpointResponse?: () => Promise<void>;

  readonly fetch: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    const body = JSON.parse(String(init?.body));
    if (url.pathname == '/sync/checkpoint-request') {
      this.checkpointRequests.push(body);
      await this.beforeCheckpointResponse?.();
      return Response.json({ data: { checkpoint_request_id: body.checkpoint_request_id } });
    }

    this.requests.push(body);
    const signal = (this.#signal = init!.signal!);
    return new Response(
      new ReadableStream({
        start: (c) => {
          this.#stream = c;
          signal.addEventListener('abort', () => {
            try {
              c.close();
            } catch {}
          });
        }
      })
    );
  };

  get aborted() {
    return this.#signal?.aborted ?? true;
  }

  get lastRequest() {
    return this.requests.at(-1)!;
  }

  async waitForRequest(count: number) {
    await vi.waitFor(() => expect(this.requests.length).toBe(count));
  }

  send(line: unknown) {
    this.#stream!.enqueue(new TextEncoder().encode(JSON.stringify(line) + '\n'));
  }

  close() {
    this.#stream!.close();
  }

  checkpoint(buckets: Array<string>, options: { streams?: Array<unknown>; writeCheckpoint?: string } = {}) {
    const request = this.lastRequest;
    this.send({
      checkpoint: {
        last_op_id: String(this.#opId + 10),
        write_checkpoint: options.writeCheckpoint,
        buckets: buckets.map((bucket) => ({ bucket, checksum: 0 })),
        streams:
          options.streams ??
          request.streams.subscriptions.map((s: { stream: string }) => ({
            name: s.stream,
            is_default: false,
            errors: []
          }))
      }
    });
  }

  put(bucket: string, table: string, id: string, data: Row) {
    this.data(bucket, [{ op: 'PUT', object_type: table, object_id: id, data: JSON.stringify(data) }]);
  }

  remove(bucket: string, table: string, id: string) {
    this.data(bucket, [{ op: 'REMOVE', object_type: table, object_id: id }]);
  }

  data(bucket: string, ops: Array<Row>) {
    this.send({
      data: { bucket, data: ops.map((op) => ({ op_id: String(++this.#opId), checksum: 0, ...op })) }
    });
  }

  complete() {
    this.send({ checkpoint_complete: { last_op_id: String(this.#opId + 10) } });
  }

  /** Sends a checkpoint with the given rows in one bucket, followed by a completion line. */
  sync(bucket: string, table: string, rows: Record<string, Row>, options?: { writeCheckpoint?: string }) {
    this.checkpoint([bucket], options);
    for (const [id, row] of Object.entries(rows)) this.put(bucket, table, id, row);
    this.complete();
  }
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

function setup() {
  const service = new FakeService();
  const client = new PowerSyncClient({
    endpoint: 'http://localhost:8080',
    getToken: () => 'token',
    fetch: service.fetch,
    retryDelayMs: 1
  });
  return { service, client };
}

function tasks(
  client: PowerSyncClient,
  options: { streams?: Array<StreamSubscription>; table?: keyof typeof APP_SCHEMA.props } = {}
) {
  const collection = createCollection(
    powerSyncCollectionOptions({
      client,
      table: APP_SCHEMA.props[options.table ?? 'tasks'],
      streams: options.streams ?? [{ name: 'tasks' }]
    })
  );
  cleanups.push(() => collection.cleanup());
  return collection;
}

it('syncs rows of its table once a checkpoint completes', async () => {
  const { service, client } = setup();
  const collection = tasks(client, { streams: [{ name: 'tasks', parameters: { board: 'work' } }] });
  const ready = collection.preload();

  await service.waitForRequest(1);
  expect(service.lastRequest.streams).toEqual({
    include_defaults: false,
    subscriptions: [{ stream: 'tasks', parameters: { board: 'work' }, override_priority: null }]
  });

  service.checkpoint(['a']);
  service.put('a', 'tasks', '1', { title: 'first' });
  service.put('a', 'other', '1', { title: 'ignored' });
  await new Promise((r) => setTimeout(r, 10));
  expect(collection.size).toBe(0);

  service.complete();
  await ready;
  expect(collection.toArray).toMatchObject([{ id: '1', title: 'first' }]);
  expect(client.status.state).toBe('ready');

  await collection.cleanup();
  expect(service.aborted).toBe(true);
  expect(client.status.state).toBe('idle');
});

it('shares one connection, requesting the union of streams', async () => {
  const { service, client } = setup();
  const first = tasks(client, { streams: [{ name: 'tasks' }] });
  const second = tasks(client, { streams: [{ name: 'lists' }], table: 'lists' });
  const ready = Promise.all([first.preload(), second.preload()]);

  await service.waitForRequest(1);
  expect(service.lastRequest.streams.subscriptions.map((s: any) => s.stream)).toEqual(['tasks', 'lists']);
  service.checkpoint(['a', 'b']);
  service.put('a', 'tasks', '1', { title: 'task' });
  service.put('b', 'lists', '1', { title: 'list' });
  service.complete();
  await ready;
  expect(first.get('1')?.title).toBe('task');
  expect(second.get('1')?.title).toBe('list');

  // A new collection on an already-synced table starts with the cached rows.
  const third = tasks(client, { streams: [] });
  await third.preload();
  expect(third.get('1')?.title).toBe('task');

  // Stopping a collection restarts with the remaining streams, resuming buckets.
  await second.cleanup();
  await service.waitForRequest(2);
  expect(service.lastRequest.streams.subscriptions.map((s: any) => s.stream)).toEqual(['tasks']);
  expect(service.lastRequest.buckets).toEqual([
    { name: 'a', after: '1' },
    { name: 'b', after: '2' }
  ]);
});

it('reports rows from streams requested by other collections', async () => {
  const { service, client } = setup();
  const requesting = tasks(client, { streams: [{ name: 'everything' }], table: 'lists' });
  const passive = tasks(client, { streams: [] });
  const ready = Promise.all([requesting.preload(), passive.preload()]);

  await service.waitForRequest(1);
  service.sync('a', 'tasks', { '1': { title: 'from another stream' } });
  await ready;
  expect(passive.get('1')?.title).toBe('from another stream');
  expect(requesting.size).toBe(0);

  service.checkpoint(['a']);
  service.remove('a', 'tasks', '1');
  service.complete();
  await vi.waitFor(() => expect(passive.size).toBe(0));
});

it('requests streams for on-demand subsets', async () => {
  const { service, client } = setup();
  const getSubscriptions = vi.fn(() => [{ name: 'board', parameters: { board: 'work' } }]);
  const collection = createCollection(
    powerSyncCollectionOptions({ client, table: APP_SCHEMA.props.tasks, syncMode: 'on-demand', getSubscriptions })
  );
  cleanups.push(() => collection.cleanup());
  await collection.preload();

  const live = createLiveQueryCollection((q) =>
    q.from({ task: collection }).where(({ task }) => eq(task.board, 'work'))
  );
  let loaded = false;
  const preload = live.preload().then(() => (loaded = true));

  await vi.waitFor(() => expect(getSubscriptions).toHaveBeenCalled());
  await vi.waitFor(() => expect(service.lastRequest.streams.subscriptions).toHaveLength(1));
  const requests = service.requests.length;
  service.sync('a', 'tasks', { '1': { title: 'task', board: 'work' } });
  await preload;
  expect(loaded).toBe(true);
  expect(live.toArray.map((t) => t.title)).toEqual(['task']);

  await live.cleanup();
  await service.waitForRequest(requests + 1);
  expect(service.lastRequest.streams.subscriptions).toEqual([]);
});

it('confirms writes once the requested checkpoint is applied', async () => {
  const { service, client } = setup();
  const collection = tasks(client);
  const ready = collection.preload();
  await service.waitForRequest(1);
  service.sync('a', 'tasks', { '1': { title: 'before' } });
  await ready;

  let confirmed = false;
  const confirmation = collection.utils.confirmWrites().then(() => (confirmed = true));
  await vi.waitFor(() => expect(service.checkpointRequests).toHaveLength(1));
  expect(service.checkpointRequests[0]).toMatchObject({ checkpoint_request_id: '1' });

  // A checkpoint without the write checkpoint is not applied.
  service.sync('a', 'tasks', { '1': { title: 'stale' } });
  await new Promise((r) => setTimeout(r, 10));
  expect(confirmed).toBe(false);
  expect(collection.get('1')?.title).toBe('before');

  service.sync('a', 'tasks', { '1': { title: 'after' } }, { writeCheckpoint: '1' });
  await confirmation;
  expect(collection.get('1')?.title).toBe('after');
});

it('confirms writes if the checkpoint is synced before the checkpoint request responds', async () => {
  const { service, client } = setup();
  const collection = tasks(client);
  const ready = collection.preload();
  await service.waitForRequest(1);
  service.sync('a', 'tasks', { '1': { title: 'before' } });
  await ready;

  service.beforeCheckpointResponse = async () => {
    service.sync('a', 'tasks', { '1': { title: 'after' } }, { writeCheckpoint: '1' });
    await vi.waitFor(() => expect(collection.get('1')?.title).toBe('after'));
  };

  // There is no further checkpoint, so this relies on re-checking the last one.
  await collection.utils.confirmWrites();
});

it('resumes buckets after reconnecting and handles bucket resets', async () => {
  const { service, client } = setup();
  const collection = tasks(client);
  const ready = collection.preload();
  await service.waitForRequest(1);
  service.sync('a', 'tasks', { '1': { title: 'one' }, '2': { title: 'two' } });
  await ready;

  service.close();
  await service.waitForRequest(2);
  expect(service.lastRequest.buckets).toEqual([{ name: 'a', after: '2' }]);

  // Re-sending an operation id that was already seen resets bucket state, re-downloading from scratch.
  service.checkpoint(['a']);
  service.data('a', [{ op: 'PUT', object_type: 'tasks', object_id: '1', data: '{}', op_id: '1' }]);
  await service.waitForRequest(3);
  expect(service.lastRequest.buckets).toEqual([]);

  // Row 2 is no longer part of the bucket, so it should be removed.
  service.sync('a', 'tasks', { '1': { title: 'one' } });
  await vi.waitFor(() => expect(collection.size).toBe(1));
  expect(collection.get('1')?.title).toBe('one');
});

it('reports stream errors to the affected collections', async () => {
  const { service, client } = setup();
  const errors: Array<Error> = [];
  const failing = createCollection(
    powerSyncCollectionOptions({
      client,
      table: APP_SCHEMA.props.tasks,
      streams: [{ name: 'invalid' }],
      onError: (e) => errors.push(e)
    })
  );
  cleanups.push(() => failing.cleanup());
  const working = tasks(client, { streams: [{ name: 'valid' }], table: 'lists' });
  const failed = failing.preload();
  const ready = working.preload();

  await service.waitForRequest(1);
  service.checkpoint([], {
    streams: [
      { name: 'invalid', is_default: false, errors: [{ subscription: 0, message: 'bad parameters' }] },
      { name: 'valid', is_default: false, errors: [] }
    ]
  });
  service.complete();

  await expect(failed).rejects.toThrow(StreamError);
  // The failed collection released its stream, so the client reconnects without it.
  await service.waitForRequest(2);
  expect(service.lastRequest.streams.subscriptions.map((s: any) => s.stream)).toEqual(['valid']);
  service.checkpoint([]);
  service.complete();
  await ready;
  expect(errors).toHaveLength(1);
  expect((errors[0] as StreamError).stream.name).toBe('invalid');
  expect((errors[0] as StreamError).messages).toEqual(['bad parameters']);
});

it('fails the collection when a synced row does not match the table schema', async () => {
  const { service, client } = setup();
  const errors: Array<Error> = [];
  const collection = createCollection(
    powerSyncCollectionOptions({
      client,
      table: APP_SCHEMA.props.tasks,
      streams: [{ name: 'tasks' }],
      onError: (e) => errors.push(e)
    })
  );
  cleanups.push(() => collection.cleanup());
  const ready = collection.preload();

  await service.waitForRequest(1);
  service.sync('a', 'tasks', { '1': { title: 42 } });

  await expect(ready).rejects.toThrow('title must be a string or null');
  expect(errors).toHaveLength(1);
  expect(collection.size).toBe(0);
});

it('rejects preload on initial failure instead of leaving it pending forever', async () => {
  const client = new PowerSyncClient({
    endpoint: 'http://localhost',
    getToken: () => 'token',
    fetch: async () => new Response('', { status: 401 }),
    retryDelayMs: 1000
  });
  const collection = tasks(client);
  await expect(collection.preload()).rejects.toThrow('401');
});
