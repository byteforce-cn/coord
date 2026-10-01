import { create } from "zustand"
import { persist } from "zustand/middleware"

export type Theme = "light" | "dark"

interface ThemeState {
  theme: Theme
  setTheme: (theme: Theme) => void
  toggleTheme: () => void
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set) => ({
      theme: "light",
      setTheme: (theme) => {
        set({ theme })
        const root = document.documentElement
        root.classList.remove("light", "dark")
        root.classList.add(theme)
      },
      toggleTheme: () =>
        set((state) => {
          const next = state.theme === "light" ? "dark" : "light"
          const root = document.documentElement
          root.classList.remove("light", "dark")
          root.classList.add(next)
          return { theme: next }
        }),
    }),
    { name: "coord-theme" }
  )
)
