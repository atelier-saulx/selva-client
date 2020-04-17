import SubscriptionManager from '../subsManager'
import sendUpdate from './sendUpdate'
import { Subscription } from '../'

var delayCount = 0

const sendUpdates = (subscriptionManager: SubscriptionManager) => {
  console.log(
    'SEND UPDATES - handled events:',
    subscriptionManager.incomingCount
  )

  const s = subscriptionManager.stagedForUpdates
  subscriptionManager.stagedForUpdates = new Set()
  subscriptionManager.stagedInProgess = false
  subscriptionManager.incomingCount = 0

  s.forEach(subscription => {
    subscription.inProgress = false

    console.log('try updating subscription', subscription.channel)
    sendUpdate(subscriptionManager, subscription)
      .then(v => {
        console.log('DID SEND UPDATE FOR', subscription.channel)
      })
      .catch(err => {
        console.log('WRONG ERROR IN SENDUPDATE', err)
      })
  })

  console.log('yesh done updating')

  delayCount = 0
}

const rate = 5

const delay = (subscriptionManager, time = 1000, totalTime = 0) => {
  if (totalTime < 20e3) {
    const lastIncoming = subscriptionManager.incomingCount
    delayCount++
    console.log('delay #', delayCount, lastIncoming)
    subscriptionManager.stagedTimeout = setTimeout(() => {
      const incoming = subscriptionManager.incomingCount - lastIncoming
      if (incoming / time > rate) {
        // too fast ait a bit longer
        // reset count
        // subscriptionManager.incomingCount = 0
        // increase time
        time *= 1.5

        // delay again
        subscriptionManager.stagedTimeout = setTimeout(() => {
          delay(subscriptionManager, time, totalTime + time)
        }, time)
      } else {
        // do it
        sendUpdates(subscriptionManager)
      }
    }, time)
  } else {
    console.log(
      '20 seconds pass drain',
      totalTime,
      'incoming',
      subscriptionManager.incomingCount
    )
    // do it now
    sendUpdates(subscriptionManager)
  }
}

const addUpdate = async (
  subscriptionManager: SubscriptionManager,
  subscription: Subscription,
  custom?: { type: string; payload?: any }
) => {
  if (subscription.inProgress && subscriptionManager.stagedInProgess) {
    console.log('Sub in progress')
  } else {
    // handle batch mechanism

    if (custom) {
      subscription.inProgress = true
      await sendUpdate(subscriptionManager, subscription, custom)
      subscription.inProgress = false
    } else {
      subscriptionManager.stagedForUpdates.add(subscription)
      subscription.inProgress = true

      if (!subscriptionManager.stagedInProgess) {
        subscriptionManager.stagedInProgess = true
        subscriptionManager.stagedTimeout = setTimeout(() => {
          console.log('go send')
          sendUpdates(subscriptionManager)

          // if (subscriptionManager.incomingCount < 1000) {
          //   sendUpdates(subscriptionManager)
          // } else {
          //   delay(subscriptionManager)
          // }
        }, 100)
      }
    }
  }
}

export default addUpdate
