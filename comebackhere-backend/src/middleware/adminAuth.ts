import { createHash, timingSafeEqual } from "crypto"
import type { RequestHandler } from "express"
import { ForbiddenError, ServiceMisconfiguredError, UnauthorizedError } from "../lib/errors.js"

const MAX_ADMIN_KEY_LENGTH = 512

/** Constant-time comparison; hashing first equalises lengths so none leaks. */
export function safeKeyEquals(supplied: string, configured: string): boolean {
  const a = createHash("sha256").update(supplied).digest()
  const b = createHash("sha256").update(configured).digest()
  return timingSafeEqual(a, b)
}

export const requireAdmin: RequestHandler = (req, res, next) => {
  const configuredKey = process.env.ADMIN_KEY
  if (!configuredKey) {
    next(new ServiceMisconfiguredError("ADMIN_KEY is not configured"))
    return
  }

  const requestId = typeof res.locals.requestId === "string" ? res.locals.requestId : "-"
  const suppliedKey = req.get("x-admin-key")
  if (!suppliedKey) {
    console.warn(
      `[admin-audit] requestId=${requestId} result=unauthorized reason=missing-credentials action=${req.method} ${req.path}`,
    )
    next(new UnauthorizedError("Admin credentials are required"))
    return
  }
  if (suppliedKey.length > MAX_ADMIN_KEY_LENGTH) {
    console.warn(
      `[admin-audit] requestId=${requestId} result=forbidden reason=key-too-long length=${suppliedKey.length} action=${req.method} ${req.path}`,
    )
    next(new ForbiddenError("Admin credentials are invalid"))
    return
  }
  if (!safeKeyEquals(suppliedKey, configuredKey)) {
    console.warn(
      `[admin-audit] requestId=${requestId} result=forbidden reason=invalid-credentials action=${req.method} ${req.path}`,
    )
    next(new ForbiddenError("Admin credentials are invalid"))
    return
  }

  const keyFingerprint = createHash("sha256").update(configuredKey).digest("hex").slice(0, 12)
  const adminIdentity = process.env.ADMIN_IDENTITY ?? `admin-${keyFingerprint}`
  res.locals.adminIdentity = adminIdentity
  console.info(
    `[admin-audit] requestId=${requestId} result=authorized admin=${adminIdentity} action=${req.method} ${req.path}`,
  )
  next()
}
