import { create } from "zustand"

export interface UserInfo {
  policies: string[]
  role: string
  displayName: string
  tokenAccessor: string
  tokenTtl: number
  tokenMaxTtl?: number
}

interface AuthState {
  isAuthenticated: boolean
  user: UserInfo | null
  isLoading: boolean
  login: (user: UserInfo) => void
  logout: () => void
  setLoading: (loading: boolean) => void
}

export const useAuthStore = create<AuthState>()((set) => ({
  isAuthenticated: false,
  user: null,
  isLoading: true,
  login: (user) => set({ isAuthenticated: true, user, isLoading: false }),
  logout: () => set({ isAuthenticated: false, user: null, isLoading: false }),
  setLoading: (loading) => set({ isLoading: loading }),
}))
