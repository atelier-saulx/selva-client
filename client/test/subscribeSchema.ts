import test from 'ava'
import { connect } from '../src/index'
import { start, startOrigin } from '@saulx/selva-server'
import { wait } from './assertions'
import getPort from 'get-port'

let srv
let srv2
let port: number
test.before(async () => {
  port = await getPort()
  srv = await start({
    port,
  })
  srv2 = await startOrigin({
    registry: { port },
    name: 'snurk',
  })
})

test.after(async (t) => {
  await srv2.destroy()
  await srv.destroy()
  await t.connectionsAreEmpty()
})

test.serial('basic schema based subscriptions', async (t) => {
  const client = connect({ port })

  const obssnurk = client.subscribeSchema('snurk')

  let snurkCnt = 0
  obssnurk.subscribe((x) => {
    snurkCnt++
    if (snurkCnt === 2) {
      if (!x.rootType.fields.snurk) {
        throw new Error('does not have snurk!')
      }
    }
  })
  await wait(2000)

  await client.updateSchema(
    {
      languages: ['en', 'de', 'nl'],
      rootType: {
        fields: { snurk: { type: 'string' } },
      },
    },
    'snurk'
  )

  const observable = client.subscribeSchema()
  let o1counter = 0
  const sub = observable.subscribe((d) => {
    o1counter++
  })

  await wait(2000)

  await client.updateSchema({
    languages: ['en', 'de', 'nl'],
    rootType: {
      fields: { yesh: { type: 'string' }, no: { type: 'string' } },
    },
  })

  await wait(500)

  await client.updateSchema({
    languages: ['en', 'de', 'nl'],
    rootType: {
      fields: { yesh: { type: 'string' }, no: { type: 'string' } },
    },
    types: {
      yeshType: {
        fields: {
          yesh: { type: 'string' },
        },
      },
    },
  })

  await wait(500)

  sub.unsubscribe()

  await wait(500)

  t.is(o1counter, 3)
  const observable2 = client.subscribeSchema()
  var cnt = 0
  const sub2 = observable2.subscribe((d) => {
    cnt++
  })

  await wait(500)

  t.is(cnt, 1)

  t.is(snurkCnt, 2)

  sub2.unsubscribe()

  await wait(1500)

  await client.destroy()

  await wait(1500)
})
