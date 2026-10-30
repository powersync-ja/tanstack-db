import type { StandardSchemaV1 } from '@standard-schema/spec'

/**
 * Creates a function that validates a synced row with `schema` and returns its output.
 *
 * Synced rows that fail validation cannot be applied, so the returned function throws after
 * reporting the failure to `log` and `onDeserializationError`.
 */
export function createRowDeserializer<TRow, TOutput>(
  tableName: string,
  schema: StandardSchemaV1<TRow, TOutput>,
  onDeserializationError:
    ((error: StandardSchemaV1.FailureResult) => void) | undefined,
  log?: (message: string) => void,
): (row: TRow) => TOutput {
  const reject = (failure: StandardSchemaV1.FailureResult, message: string) => {
    log?.(message)
    onDeserializationError?.(failure)
    throw new Error(message)
  }

  return (row) => {
    const validation = schema[`~standard`].validate(row)
    if (`value` in validation) {
      return validation.value
    } else if (`issues` in validation) {
      return reject(
        validation,
        `Failed to validate incoming data for ${tableName}. Issues: ${validation.issues.map((issue) => `${issue.path} - ${issue.message}`)}`,
      )
    } else {
      const message = `Unknown deserialization error for ${tableName}`
      return reject({ issues: [{ message }] }, message)
    }
  }
}
