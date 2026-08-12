export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { startResultsListener } = await import("./server/redis-listener");
    await startResultsListener();
  }
}
