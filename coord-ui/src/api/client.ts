interface ApiResponse<T> {
  code: number
  data: T
  message: string
}

const BASE_URL = "/api/v1"

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const url = `${BASE_URL}${path}`
  const res = await fetch(url, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    ...options,
  })

  if (!res.ok) {
    const body = await res.json().catch(() => ({ message: res.statusText }))
    throw new ApiError(res.status, body.message || res.statusText)
  }

  const json: ApiResponse<T> = await res.json()
  if (json.code !== 0) {
    throw new ApiError(json.code, json.message)
  }
  return json.data
}

export class ApiError extends Error {
  code: number
  constructor(code: number, message: string) {
    super(message)
    this.name = "ApiError"
    this.code = code
  }
}

export const api = {
  auth: {
    login: (roleId: string, secretId: string) =>
      request<{
        policies: string[]
        role: string
        displayName: string
        tokenAccessor: string
        tokenTtl: number
        tokenMaxTtl: number
      }>("/auth/login", {
        method: "POST",
        body: JSON.stringify({ roleId, secretId }),
      }),

    renew: () =>
      request<{ tokenTtl: number }>("/auth/renew", { method: "POST" }),

    revoke: () =>
      request<void>("/auth/revoke", { method: "POST" }),

    userinfo: () =>
      request<{
        policies: string[]
        role: string
        displayName: string
        tokenAccessor: string
        tokenTtl: number
      }>("/auth/userinfo"),
  },

  registry: {
    list: (params?: { q?: string; status?: string; page?: number; pageSize?: number }) => {
      const searchParams = new URLSearchParams()
      if (params?.q) searchParams.set("q", params.q)
      if (params?.status && params.status !== "all") searchParams.set("status", params.status)
      if (params?.page) searchParams.set("page", String(params.page))
      if (params?.pageSize) searchParams.set("pageSize", String(params.pageSize))
      const qs = searchParams.toString()
      return request<{
        services: Array<{
          name: string
          tags: string[]
          status: string
          instances: { total: number; healthy: number; warning: number; critical: number }
          address: string
          port: number
        }>
        total: number
      }>(`/registry/services${qs ? `?${qs}` : ""}`)
    },

    detail: (name: string) =>
      request<{
        name: string
        tags: string[]
        healthRate: number
        instances: Array<{
          id: string
          address: string
          port: number
          status: string
          lastCheck: string
        }>
      }>(`/registry/services/${encodeURIComponent(name)}`),

    updateInstance: (serviceName: string, instanceId: string, status: string) =>
      request<void>(`/registry/services/${encodeURIComponent(serviceName)}/instances/${encodeURIComponent(instanceId)}`, {
        method: "PUT",
        body: JSON.stringify({ status }),
      }),

    healthCheck: (serviceName: string) =>
      request<void>(`/registry/services/${encodeURIComponent(serviceName)}/health-check`, {
        method: "POST",
      }),
  },

  config: {
    list: (params?: { group?: string; q?: string; page?: number; pageSize?: number }) => {
      const searchParams = new URLSearchParams()
      if (params?.group) searchParams.set("group", params.group)
      if (params?.q) searchParams.set("q", params.q)
      if (params?.page) searchParams.set("page", String(params.page))
      if (params?.pageSize) searchParams.set("pageSize", String(params.pageSize))
      const qs = searchParams.toString()
      return request<{
        configs: Array<{
          group: string
          key: string
          format: string
          version: number
          updatedAt: string
          updatedBy: string
        }>
        total: number
      }>(`/configs${qs ? `?${qs}` : ""}`)
    },

    detail: (group: string, key: string) =>
      request<{
        group: string
        key: string
        version: number
        createdAt: string
        updatedAt: string
        updatedBy: string
        changeNote: string
        format: string
        data: string
      }>(`/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}`),

    create: (body: { group: string; key: string; format: string; data: string; changeNote?: string }) =>
      request<{ version: number }>("/configs", { method: "POST", body: JSON.stringify(body) }),

    update: (group: string, key: string, body: { data: string; version: number; changeNote?: string }) =>
      request<{ version: number }>(`/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}`, {
        method: "PUT",
        body: JSON.stringify(body),
      }),

    delete: (group: string, key: string) =>
      request<void>(`/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}`, { method: "DELETE" }),

    versions: (group: string, key: string) =>
      request<Array<{ version: number; updatedAt: string; updatedBy: string; changeNote: string }>>(
        `/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}/versions`
      ),

    versionDetail: (group: string, key: string, version: number) =>
      request<{ group: string; key: string; version: number; createdAt: string; format: string; data: string }>(
        `/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}/versions/${version}`
      ),

    rollback: (group: string, key: string, version: number) =>
      request<{ newVersion: number }>(
        `/configs/${encodeURIComponent(group)}/${encodeURIComponent(key)}/rollback`,
        { method: "POST", body: JSON.stringify({ version }) }
      ),
  },
}
