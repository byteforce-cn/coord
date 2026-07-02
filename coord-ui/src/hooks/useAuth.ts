import { useMutation, useQueryClient } from "@tanstack/react-query"
import { useNavigate } from "@tanstack/react-router"
import { api } from "@/api/client"
import { useAuthStore } from "@/stores/authStore"

export function useLogin() {
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const loginStore = useAuthStore((s) => s.login)

  return useMutation({
    mutationFn: (creds: { roleId: string; secretId: string }) => api.auth.login(creds.roleId, creds.secretId),
    onSuccess: (data) => {
      loginStore(data)
      queryClient.setQueryData(["auth"], data)
      navigate({ to: "/dashboard" })
    },
  })
}

export function useLogout() {
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const logoutStore = useAuthStore((s) => s.logout)

  return useMutation({
    mutationFn: () => api.auth.revoke(),
    onSuccess: () => {
      logoutStore()
      queryClient.clear()
      navigate({ to: "/login" })
    },
    onError: () => {
      // Even if revoke fails, clear local state
      logoutStore()
      queryClient.clear()
      navigate({ to: "/login" })
    },
  })
}

export function useUserInfo() {
  const { isAuthenticated, login, logout, setLoading } = useAuthStore()

  const checkAuth = async () => {
    try {
      setLoading(true)
      const user = await api.auth.userinfo()
      login(user)
    } catch {
      logout()
    }
  }

  return { isAuthenticated, checkAuth }
}
