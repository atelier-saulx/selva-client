import Observable from '../observe/observable'
import { GetOptions, GetResult } from '../get/types'
import { LogFn, SelvaOptions, SelvaClient } from '..'
import {
  createClient,
  RedisWrapper,
  clientTypes,
  ClientType
} from './redisWrapper'
import { Event, RedisCommand, UpdateEvent, DeleteEvent } from './types'
import { EventEmitter } from 'events'
// adds FT commands to redis
import './redisSearch'
import RedisMethodsByType from './redisMethodsByType'
import RedisMethods from './RedisMethods'

class Subscription extends EventEmitter {
  public channel: string
  public getOpts: GetOptions
  public count: number = 0
  constructor(getOpts: GetOptions) {
    super()
    this.getOpts = getOpts
  }
}

export type ConnectOptions = {
  port: number
  host?: string
  subscriptions?: {
    port: number
    host?: string
  }
}

const defaultLogging: LogFn = log => {
  if (log.level === 'warning') {
    console.warn('LUA: ' + log.msg)
  } else {
    console[log.level]('LUA: ' + log.msg)
  }
}

export default class RedisClient extends RedisMethods {
  public connector: () => Promise<ConnectOptions>
  public isDestroyed: boolean

  private clientId: string
  private log: LogFn | undefined
  private selvaClient: SelvaClient
  private connectOptions: ConnectOptions
  public subscriptions: { [channel: string]: Subscription } = {}

  public redis: RedisWrapper

  public buffer: { [client: string]: RedisCommand[] } = {}
  public connected: { [client: string]: boolean } = {}

  public byType: RedisMethodsByType

  constructor(
    connect:
      | ConnectOptions
      | (() => Promise<ConnectOptions>)
      | Promise<ConnectOptions>,
    selvaClient: SelvaClient,
    selvaOpts: SelvaOptions
  ) {
    super()

    clientTypes.forEach(type => {
      this.buffer[type] = []
      this.connected[type] = false
    })

    this.byType = new RedisMethodsByType(this)

    this.clientId = selvaClient.clientId
    this.selvaClient = selvaClient
    this.log =
      (selvaOpts && selvaOpts.log) ||
      (selvaOpts && selvaOpts.loglevel && selvaOpts.loglevel !== 'off'
        ? defaultLogging
        : undefined)

    this.connector =
      typeof connect === 'object'
        ? () => Promise.resolve(connect)
        : connect instanceof Promise
        ? async () => connect
        : connect
    this.connect()
  }

  destroy() {
    this.isDestroyed = true
    this.redis.removeClient(this.clientId)
    for (let channel in this.subscriptions) {
      this.subscriptions[channel].removeAllListeners()
      delete this.subscriptions[channel]
    }
    this.redis = null
    this.connected = {}
    this.buffer = {}
    clientTypes.forEach(type => {
      this.buffer[type] = []
    })
  }

  createSubscription(channel: string, getOpts: GetOptions) {
    const subscription = (this.subscriptions[channel] = new Subscription(
      getOpts
    ))

    if (this.redis) {
      this.redis.subscribe(this.clientId, channel, getOpts)
    }
    return subscription
  }

  subscribe(channel: string, getOpts: GetOptions): Observable<GetResult> {
    const subscription =
      this.subscriptions[channel] || this.createSubscription(channel, getOpts)
    subscription.count++
    return new Observable(observer => {
      const listener = (event: UpdateEvent | DeleteEvent) => {
        if (event.type === 'update') {
          if (observer.version !== event.version) {
            observer.version = event.version
            observer.next(event.payload)
          }
        } else if (event.type === 'delete') {
          observer.next(null)
        }
      }
      subscription.on('message', listener)

      if (
        this.redis &&
        this.redis.allConnected &&
        this.redis.subscriptions[channel] &&
        this.redis.subscriptions[channel].version
      ) {
        this.redis.emitChannel(channel, this.clientId)
      }

      return () => {
        subscription.count--
        subscription.removeListener('message', listener)
        if (subscription.count === 0) {
          this.redis.unsubscribe(this.clientId, channel)
          subscription.removeAllListeners()
          delete this.subscriptions[channel]
        }
      }
    })
  }

  private async registerSubscriptions() {
    for (const channel in this.subscriptions) {
      this.redis.subscribe(
        this.clientId,
        channel,
        this.subscriptions[channel].getOpts
      )
    }
  }

  loadAndEvalScript(
    scriptName: string,
    script: string,
    numKeys: number,
    keys: string[],
    args: string[],
    type: ClientType,
    opts?: { batchingEnabled?: boolean }
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      this.queue(
        'loadAndEvalScript',
        [
          opts && opts.batchingEnabled ? 1 : 0,
          script,
          scriptName,
          numKeys,
          ...keys,
          ...args
        ],
        resolve,
        reject,
        type
      )
    })
  }

  async queue(
    command: string,
    args: (string | number)[],
    resolve: (x: any) => void = () => {},
    reject: (x: Error) => void = () => {},
    type?: ClientType
  ) {
    if (this.isDestroyed) {
      console.error('Trying to set on a destroyed client!', command)
      return
    }
    // remove type
    if (type === undefined) {
      if (command === 'subscribe') {
        type = 'sSub'
      } else if (command === 'publish') {
        type = 'sClient'
      } else {
        type = 'client'
      }
    }
    if (this.connected[type]) {
      this.redis.queue(command, args, resolve, reject, type)
    } else {
      if (!this.buffer[type]) {
        console.error('Cannot find type on buffer', type)
      }

      this.buffer[type].push({
        command,
        args,
        resolve,
        reject
      })
    }
  }

  public sendQueueToRedis(type) {
    // need to check if dc here else you loose commands
    const buffer = this.buffer[type]
    this.buffer[type] = []

    buffer.forEach(({ command, args, resolve, reject }) => {
      this.redis.queue(command, args, resolve, reject, type)
    })
  }

  private async connect() {
    if (this.isDestroyed) {
      return
    }
    const opts = await this.connector()
    this.connectOptions = opts
    this.redis = createClient(opts)
    this.registerSubscriptions()

    this.redis.addClient(this.clientId, {
      message: (channel, message) => {
        const subscription = this.subscriptions[channel]
        if (subscription && message.type) {
          subscription.emit('message', message)
        }
      },
      log: this.log,
      connect: type => {
        this.connected[type] = true
        this.sendQueueToRedis(type)
        if (this.redis.allConnected) {
          this.selvaClient.emit('connect')
        }
      },
      disconnect: type => {
        this.connected[type] = false
        if (this.redis.allDisconnected) {
          this.selvaClient.emit('disconnect')
        }
        if (type === 'client') {
          this.connector().then(opts => {
            if (
              opts.host !== this.connectOptions.host ||
              opts.port !== this.connectOptions.port ||
              (opts.subscriptions && !this.connectOptions.subscriptions) ||
              (this.connectOptions.subscriptions && !opts.subscriptions) ||
              (opts.subscriptions &&
                this.connectOptions.subscriptions &&
                (opts.subscriptions.host !==
                  this.connectOptions.subscriptions.host ||
                  opts.subscriptions.port !==
                    this.connectOptions.subscriptions.port))
            ) {
              this.redis.removeClient(this.clientId)
              this.connect()
            }
          })
        }
      },
      client: this
    })

    this.selvaClient.subscribeSchema()
  }
}
