import { promises as fs } from 'fs'
import { join as pathJoin } from 'path'
import { SelvaClient } from '@saulx/selva'

let LAST_BACKUP_TIMESTAMP: number = 0
let LAST_RUN: number = Date.now()

function msSinceMidnight(d: Date = new Date()) {
  d.setHours(0, 0, 0, 0)
  return Date.now() - d.getTime()
}

function nextBackupTime(
  lastBackupTime: number,
  backupInterval: number
): number {
  return Math.ceil(lastBackupTime / backupInterval) * backupInterval
}

export type BackupFns = { sendBackup: SendBackup; loadBackup: LoadBackup }
export type SendBackup = (rdbFilePath: string) => Promise<void> // upload backup to cloud
export type LoadBackup = (
  rdbFilePath: string,
  rdbLastModified?: Date
) => Promise<void> // overwrite local dump.rdb with latest cloud backup

export async function loadBackup(redisDir: string, backupFns: BackupFns) {
  const dumpFile = pathJoin(redisDir, 'dump.rdb')
  try {
    let stat = await fs.stat(dumpFile)
    console.info(`Local dump found from ${stat.mtime} of ${stat.size} bytes`)
    await backupFns.loadBackup(dumpFile, stat.mtime)
    stat = await fs.stat(dumpFile)
    console.info(`Backup load completed, size: ${stat.size} bytes`)
  } catch (e) {
    await backupFns.loadBackup(dumpFile)
  }
}

export async function saveAndBackUp(
  client: SelvaClient,
  redisDir: string,
  backupFns: BackupFns
): Promise<void> {
  try {
    await client.redis.save()
    await backupFns.sendBackup(pathJoin(redisDir, 'dump.rdb'))
  } catch (e) {
    console.error(`Failed to back up ${e.stack}`)
    throw e
  }
}

export function scheduleBackups(
  redisDir: string,
  intervalInMinutes: number,
  backupFns: BackupFns
) {
  let timeout = null
  const backup = () => {
    console.info(`Scheduling backup in ${intervalInMinutes} minutes`)
    timeout = setTimeout(() => {
      runBackup(redisDir, backupFns)
        .then(() => {
          console.info('Backup successfully created')
        })
        .catch((e) => {
          console.info('error', e)
        })
        .finally(() => {
          backup()
        })
    }, intervalInMinutes * 60 * 1e3)
  }

  backup()

  return () => {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}

export async function runBackup(redisDir: string, backupFns: BackupFns) {
  const dumpPath = pathJoin(redisDir, 'dump.rdb')
  const timeOfDay = msSinceMidnight()

  LAST_RUN = Date.now()

  const stat = await fs.stat(dumpPath)
  if (stat.mtime < new Date(LAST_BACKUP_TIMESTAMP)) {
    console.info(`No changes since ${stat.mtime}, skipping backup`)
    return
  }

  console.info('Trying to create backup', String(timeOfDay))

  await backupFns.sendBackup(dumpPath)
  LAST_BACKUP_TIMESTAMP = Date.now()
}
