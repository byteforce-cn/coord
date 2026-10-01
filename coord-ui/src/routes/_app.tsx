import { createFileRoute, redirect } from "@tanstack/react-router"
import { useAuthStore } from "@/stores/authStore"
import { AppLayout } from "@/components/layout/AppLayout"

export const Route = createFileRoute("/_app")({
  beforeLoad: ({ location }) => {
    const isAuthenticated = useAuthStore.getState().isAuthenticated
    if (!isAuthenticated) {
      throw redirect({
        to: "/login",
        search: { redirect: location.href },
      })
    }
  },
  component: AppLayout,
})
