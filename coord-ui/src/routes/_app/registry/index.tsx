import { createFileRoute } from "@tanstack/react-router"
import { z } from "zod"
import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { api } from "@/api/client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Network, RefreshCw, Search, ChevronLeft, ChevronRight } from "lucide-react"
import { Link } from "@tanstack/react-router"

const searchSchema = z.object({
  q: z.string().optional(),
  status: z.enum(["passing", "warning", "critical", "all"]).default("all"),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().min(10).max(100).default(20),
})

export const Route = createFileRoute("/_app/registry/")({
  validateSearch: searchSchema,
  component: RegistryListPage,
})

const statusColors: Record<string, "success" | "warning" | "destructive" | "default"> = {
  passing: "success",
  warning: "warning",
  critical: "destructive",
}

const statusLabels: Record<string, string> = {
  passing: "健康",
  warning: "警告",
  critical: "异常",
}

function RegistryListPage() {
  const { q, status, page, pageSize } = Route.useSearch()
  const navigate = Route.useNavigate()
  const [searchInput, setSearchInput] = useState(q || "")

  const { data, isLoading, refetch } = useQuery({
    queryKey: ["registry", "services", { q, status, page, pageSize }],
    queryFn: () => api.registry.list({ q, status, page, pageSize }),
  })

  const handleSearch = () => {
    navigate({ search: (prev) => ({ ...prev, q: searchInput || undefined, page: 1 }) })
  }

  const handleStatusFilter = (value: string) => {
    navigate({
      search: (prev) => ({
        ...prev,
        status: value as typeof status,
        page: 1,
      }),
    })
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">服务注册中心</h1>
          <p className="text-muted-foreground">管理已注册的服务与实例</p>
        </div>
        <Button variant="outline" size="sm" onClick={() => refetch()}>
          <RefreshCw className="mr-2 h-4 w-4" />
          刷新
        </Button>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="搜索服务名..."
            value={searchInput}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchInput(e.target.value)}
            onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => e.key === "Enter" && handleSearch()}
            className="pl-9"
          />
        </div>
        <Select value={status} onValueChange={handleStatusFilter}>
          <SelectTrigger className="w-[140px]">
            <SelectValue placeholder="状态过滤" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部状态</SelectItem>
            <SelectItem value="passing">健康</SelectItem>
            <SelectItem value="warning">警告</SelectItem>
            <SelectItem value="critical">异常</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Service Grid */}
      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <Card key={i} className="animate-pulse">
              <CardContent className="p-6">
                <div className="h-5 w-32 rounded bg-muted" />
                <div className="mt-3 h-4 w-24 rounded bg-muted" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : data && data.services.length > 0 ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {data.services.map((svc) => (
            <Link
              key={svc.name}
              to="/registry/$serviceName"
              params={{ serviceName: svc.name }}
              className="block transition-colors hover:no-underline"
            >
              <Card className="h-full transition-shadow hover:shadow-md cursor-pointer">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <Network className="h-5 w-5 text-primary" />
                      <CardTitle className="text-lg">{svc.name}</CardTitle>
                    </div>
                    <Badge variant={statusColors[svc.status] || "default"}>
                      {statusLabels[svc.status] || svc.status}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    <div className="flex gap-1 flex-wrap">
                      {svc.tags.map((tag: string) => (
                        <Badge key={tag} variant="secondary" className="text-xs">
                          {tag}
                        </Badge>
                      ))}
                    </div>
                    <div className="flex gap-4 text-sm text-muted-foreground">
                      <span>总实例: {svc.instances.total}</span>
                      <span className="text-green-600 dark:text-green-400">
                        健康: {svc.instances.healthy}
                      </span>
                      {svc.instances.warning > 0 && (
                        <span className="text-yellow-600 dark:text-yellow-400">
                          警告: {svc.instances.warning}
                        </span>
                      )}
                      {svc.instances.critical > 0 && (
                        <span className="text-red-600 dark:text-red-400">
                          异常: {svc.instances.critical}
                        </span>
                      )}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      ) : (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12 text-center">
            <Network className="mb-4 h-12 w-12 text-muted-foreground/50" />
            <p className="text-muted-foreground">暂无注册服务</p>
          </CardContent>
        </Card>
      )}

      {/* Pagination */}
      {data && data.total > 0 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            共 {data.total} 个服务，第 {page} / {Math.ceil(data.total / pageSize)} 页
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={page <= 1}
              onClick={() => navigate({ search: (prev) => ({ ...prev, page: page - 1 }) })}
            >
              <ChevronLeft className="mr-1 h-4 w-4" />
              上一页
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= Math.ceil(data.total / pageSize)}
              onClick={() => navigate({ search: (prev) => ({ ...prev, page: page + 1 }) })}
            >
              下一页
              <ChevronRight className="ml-1 h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
