import { Link, useLocation } from "@tanstack/react-router"
import { cn } from "@/lib/utils"
import {
  LayoutDashboard,
  Network,
  Settings,
  Shield,
  HardDrive,
  MessageSquare,
  Workflow,
  ChevronLeft,
  ChevronRight,
} from "lucide-react"
import { useState } from "react"
import { Button } from "@/components/ui/button"

interface NavItem {
  title: string
  href: string
  icon: React.ComponentType<{ className?: string }>
  policies?: string[]
}

const navItems: NavItem[] = [
  { title: "仪表盘", href: "/dashboard", icon: LayoutDashboard },
  { title: "服务注册", href: "/registry", icon: Network },
  { title: "配置中心", href: "/config", icon: Settings },
  { title: "安全", href: "/security", icon: Shield, policies: ["admin"] },
  { title: "缓存", href: "/cache", icon: HardDrive },
  { title: "消息队列", href: "/mq", icon: MessageSquare },
  { title: "工作流", href: "/workflow", icon: Workflow },
]

export function Sidebar() {
  const [collapsed, setCollapsed] = useState(false)
  const location = useLocation()

  const isActive = (href: string) => {
    if (href === "/dashboard") return location.pathname === href
    return location.pathname.startsWith(href)
  }

  return (
    <aside
      className={cn(
        "flex flex-col border-r bg-sidebar text-sidebar-foreground transition-all duration-300",
        collapsed ? "w-16" : "w-60"
      )}
    >
      {/* Logo */}
      <div className="flex h-14 items-center justify-between border-b border-sidebar-border px-3">
        {!collapsed && <span className="text-lg font-bold tracking-tight">Coord</span>}
        <Button
          variant="ghost"
          size="icon"
          className="text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
          onClick={() => setCollapsed(!collapsed)}
          aria-label={collapsed ? "展开侧边栏" : "折叠侧边栏"}
        >
          {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
        </Button>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 overflow-y-auto p-2">
        {navItems.map((item) => (
          <Link
            key={item.href}
            to={item.href as never}
            className={cn(
              "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
              isActive(item.href)
                ? "bg-sidebar-accent text-sidebar-accent-foreground"
                : "text-sidebar-foreground/70",
              collapsed && "justify-center px-2"
            )}
          >
            <item.icon className="h-5 w-5 shrink-0" />
            {!collapsed && <span>{item.title}</span>}
          </Link>
        ))}
      </nav>

      {/* Footer */}
      {!collapsed && (
        <div className="border-t border-sidebar-border p-3 text-xs text-sidebar-foreground/50">
          Coord v8.1.0
        </div>
      )}
    </aside>
  )
}
