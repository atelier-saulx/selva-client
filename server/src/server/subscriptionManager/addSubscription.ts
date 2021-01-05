import { SubscriptionManager, Subscription } from './types'
import { constants, GetOptions } from '@saulx/selva'
import { hash } from './util'
import addUpdate from './update/addUpdate'
import { addOriginListeners } from './originListeners'
import updateRegistry from './updateRegistrySubscriptions'
import { addRefreshMeta } from './updateRefresh'

const { CACHE, SUBSCRIPTIONS } = constants

const addClientSubscription = async (
  subsManager: SubscriptionManager,
  client: string,
  channel: string
) => {
  const { selector } = subsManager
  const redis = subsManager.client.redis
  if (!subsManager.subscriptions[channel]) {
    const [getOptions, clients] = await Promise.all([
      redis.hget(selector, SUBSCRIPTIONS, channel),
      redis.smembers(selector, channel)
    ])
    if (getOptions && clients.length) {
      if (subsManager.subscriptions[channel]) {
        subsManager.subscriptions[channel].clients.add(client)
      } else {
        addSubscription(
          subsManager,
          channel,
          new Set(clients),
          JSON.parse(getOptions)
        )
      }
    }
  } else {
    subsManager.subscriptions[channel].clients.add(client)
  }
}

const parseOrigins = (
  channel: string,
  getOptions: GetOptions,
  origins?: Set<string>
): Set<string> => {
  if (channel.startsWith('___selva_subscription:schema_update:')) {
    return new Set([channel.split('___selva_subscription:schema_update:')[1]])
  } else if (!origins) {
    origins = new Set()
    if (!getOptions.$db) {
      origins.add('default')
    }
  }
  for (let key in getOptions) {
    if (key === '$db') {
      origins.add(getOptions[key])
    } else if (typeof getOptions[key] === 'object') {
      parseOrigins(channel, getOptions[key], origins)
    }
  }
  return origins
}

const updateSubscription = async (
  subsManager: SubscriptionManager,
  channel: string,
  subscription: Subscription
) => {
  const { selector, client } = subsManager
  const { redis } = client
  if (subsManager.subscriptions[channel] === subscription) {
    if (await redis.hexists(selector, CACHE, channel)) {
      if (subsManager.subscriptions[channel] === subscription) {
        const [tree, version] = await redis.hmget(
          selector,
          CACHE,
          channel + '_tree',
          channel + '_version'
        )
        if (subsManager.subscriptions[channel] === subscription) {
          if (!tree) {
            addUpdate(subsManager, subscription)
          } else {
            subsManager.subscriptions[channel].version = version
            subsManager.subscriptions[channel].tree = JSON.parse(tree)
            addRefreshMeta(subsManager, subscription)
          }
        }
      }
    } else {
      if (subsManager.subscriptions[channel] === subscription) {
        addUpdate(subsManager, subscription)
      }
    }
  }
}

const addSubscription = (
  subsManager: SubscriptionManager,
  channel: string,
  clients: Set<string>,
  getOptions: GetOptions
) => {
  const subscription: Subscription = {
    originDescriptors: {},
    clients,
    channel,
    get: getOptions,
    origins: [...parseOrigins(channel, getOptions).values()]
  }

  subsManager.subscriptions[channel] = subscription
  for (const origin of subscription.origins) {
    addOriginListeners(origin, subsManager, subscription)
  }

  // TODO: refactor this!
  updateRegistry(
    subsManager.client,
    {
      ...subsManager.selector,
      subscriptions: { [channel]: 'created' }
    },
    subsManager
  )
  updateSubscription(subsManager, channel, subscription)
}

export { addSubscription, addClientSubscription }
