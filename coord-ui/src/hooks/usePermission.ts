import { useAuthStore } from "@/stores/authStore"

export function usePermission() {
  const user = useAuthStore((s) => s.user)

  const hasPolicy = (policy: string): boolean => {
    if (!user) return false
    return user.policies.includes(policy) || user.policies.includes("admin")
  }

  const canWrite = (): boolean => {
    return hasPolicy("admin") || hasPolicy("write")
  }

  const canRead = (): boolean => {
    return hasPolicy("admin") || hasPolicy("write") || hasPolicy("read") || hasPolicy("ui-access")
  }

  return { hasPolicy, canWrite, canRead, policies: user?.policies ?? [], role: user?.role ?? "" }
}
