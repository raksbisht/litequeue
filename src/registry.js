let defaultQueue = null

export function setDefaultQueue(queue) {
  defaultQueue = queue
}

export function getDefaultQueue() {
  if (!defaultQueue) {
    throw new Error('No queue has been created yet. Call createQueue() before dispatching jobs.')
  }
  return defaultQueue
}
