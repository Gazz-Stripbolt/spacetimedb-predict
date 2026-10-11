// A TCP proxy that adds latency in both directions, preserving order. Used to make the server
// feel far away.
//
//   npx tsx tests/proxy.mts 3100 3000 150     (listen on :3100, forward to :3000, +150 ms each way)
import net from 'node:net';

export function startProxy(listen: number, target: number, delayMs: number): Promise<net.Server> {
  const server = net.createServer((client) => {
    const upstream = net.connect(target, '127.0.0.1');
    const pipe = (from: net.Socket, to: net.Socket) => {
      from.on('data', (chunk) => {
        // Same delay for every chunk, so setTimeout keeps them in order.
        setTimeout(() => to.destroyed || to.write(chunk), delayMs);
      });
      from.on('close', () => setTimeout(() => to.destroy(), delayMs));
      from.on('error', () => to.destroy());
    };
    pipe(client, upstream);
    pipe(upstream, client);
  });
  return new Promise((resolve) => server.listen(listen, '127.0.0.1', () => resolve(server)));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [listen = '3100', target = '3000', delay = '150'] = process.argv.slice(2);
  await startProxy(Number(listen), Number(target), Number(delay));
  console.log(`proxy :${listen} -> :${target}, +${delay} ms each way`);
}
