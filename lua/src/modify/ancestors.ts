import { Id } from '../../../src/schema/index'
import * as redis from '../redis'
import * as logger from '../logger'
import { splitString, joinString } from '../util'
import { addFieldToSearch } from './search'

// order by depth (highest depth first)

// 2league,1root,1tag   //put

// zadd
// if 2 thing with same depth use sort

// includeAncestryWith
// excludeAncestryWith

// hierachy: false

// going trough all

// -- zSet keys

// change this .ancestor

// map {[id]}

// get an array of ids

//

const needAncestorUpdates: Record<Id, true> = {}
const depthMap: Record<Id, number> = {}

export function markForAncestorRecalculation(id: Id) {
  needAncestorUpdates[id] = true
}

function getDepth(id: Id): number | false {
  if (id === 'root') {
    return 0
  }

  if (depthMap[id]) {
    return depthMap[id]
  }

  const depth = tonumber(redis.get(id + '._depth'))
  if (!depth) {
    return false
  }

  return depth
}

function setDepth(id: Id, depth: number): void {
  if (depthMap[id] === depth) {
    // cache not changed, bail
    return
  }

  depthMap[id] = depth
  redis.set(id + '._depth', tostring(depth))
}

// we need to treat depth as the min depth of all ancestors + 1
function updateDepths(id: Id): void {
  // update self depth
  const parents = redis.smembers(id + '.parents')
  let maxParentDepth: number | null = null
  for (const parent of parents) {
    let parentDepth = getDepth(parent)
    if (!parentDepth) {
      parentDepth = 0
    }

    if (parentDepth && (!maxParentDepth || maxParentDepth < parentDepth)) {
      maxParentDepth = parentDepth
    }
  }

  if (!maxParentDepth) {
    maxParentDepth = 0
  }
  logger.info(`maxParentDepth for id ${id} = ${maxParentDepth}`)

  setDepth(id, 1 + maxParentDepth)
  logger.info(`depth updated ${cjson.encode(depthMap)}`)

  // update depth of all children
  const children = redis.smembers(id + '.children')
  if (!children) {
    return
  }

  for (const child of children) {
    updateDepths(child)
    // update the depth of self in child ancestors
    redis.zAddReplaceScore(child + '.ancestors', 1 + maxParentDepth, id)
  }
}

function reCalculateAncestorsFor(ids: Id[]): void {
  logger.info(`reCalculateAncestors ${cjson.encode(ids)}`)

  // we want to update ancestors frow lowest to deepest
  table.sort(ids, (a, b) => {
    return (getDepth(a) || 0) <= (getDepth(b) || 0)
  })
  logger.info(`sorted ids by depth ${cjson.encode(ids)}`)

  for (const id of ids) {
    // clear the ancestors in case of any removed ancestors
    redis.del(id + '.ancestors')

    const parents = redis.smembers(id + '.parents')
    if (parents) {
      for (const parent of parents) {
        // add all ancestors of parent
        const parentAncestorKey = parent + '.ancestors'
        const parentAncestors: string[] = redis.zrangeWithScores(
          parentAncestorKey
        )
        logger.info(`parent ancestors for ${parent}`)
        logger.info(parentAncestors)

        const reversed: string[] = []
        for (let i = 0; i < parentAncestors.length; i += 2) {
          // reversed[i] = tostring(1 + tonumber(parentAncestors[i + 1]))
          reversed[i] = parentAncestors[i + 1]
          reversed[i + 1] = parentAncestors[i]
        }

        // add root if no ancestors in parent (it's the root)
        if (reversed.length === 0) {
          reversed[0] = '0'
          reversed[1] = 'root'
        }

        logger.info(`yesh`)
        logger.info(
          `zAddMultipleNew ${id + '.ancestors'}, ${cjson.encode(reversed)}`
        )
        redis.zAddMultipleNew(id + '.ancestors', ...reversed)

        // set parent itself into the ancestry
        const parentDepth = getDepth(parent)
        if (parentDepth) {
          // if not root
          redis.zaddNew(id + '.ancestors', parentDepth, parent)
        }
      }
    }

    const children = redis.smembers(id + '.children')
    if (children) {
      reCalculateAncestorsFor(children)
    }
  }
  // TODO: add this functionality
  // let ancestors = getNewAncestors(parents)
  // const stringAncestors = joinString(ancestors, ',')
  // redis.hset(id, 'ancestors', stringAncestors)
  // addFieldToSearch(id, 'ancestors', stringAncestors)
  // const children = redis.smembers(id + '.children')
  // for (let child of children) {
  //   reCalculateAncestors(child)
  // }
}

export function reCalculateAncestors(): void {
  const ids: Id[] = []
  for (const id in needAncestorUpdates) {
    updateDepths(id)
    ids[ids.length] = id
  }
  logger.info(`depth map ${cjson.encode(depthMap)}`)

  reCalculateAncestorsFor(ids)
}
