import { createFileRoute } from "@tanstack/react-router"
import { z } from "zod"
import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { api } from "@/api/client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Plus, Search, Settings, ChevronLeft, ChevronRight } from "lucide-react"
import { Link } from "@tanstack/react-router"

const searchSchema = z.object({
  group: z.string().optional(),
  q: z.string().optional(),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().min(10).max(100).default(20),
})

export const Route = createFileRoute("/_app/config/")({
  validateSearch: searchSchema,
  component: ConfigListPage,
})

function ConfigListPage() {
  const { group, q, page, pageSize } = Route.useSearch()
  const navigate = Route.useNavigate()
  const [searchInput, setSearchInput] = useState(q || "")
  const [groupInput, setGroupInput] = useState(group || "")

  const { data, isLoading } = useQuery({
    queryKey: ["config", "list", { group, q, page, pageSize }],
    queryFn: () => api.config.list({ group, q, page, pageSize }),
  })

  const handleSearch = () => {
    navigate({
      search: (prev) => ({
        ...prev,
        q: searchInput || undefined,
        group: groupInput || undefined,
        page: 1,
      }),
    })
  }

  const formatLabels: Record<string, string> = {
    yaml: "YAML",
    json: "JSON",
    toml: "TOML",
    text: "TEXT",
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">配置中心</h1>
          <p className="text-muted-foreground">管理分布式配置项</p>
        </div>
        <Button asChild>
          <Link to="/config/create">
            <Plus className="mr-2 h-4 w-4" />
            新建配置
          </Link>
        </Button>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="搜索配置 Key..."
            value={searchInput}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchInput(e.target.value)}
            onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => e.key === "Enter" && handleSearch()}
            className="pl-9"
          />
        </div>
        <Input
          placeholder="Group 筛选..."
          value={groupInput}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGroupInput(e.target.value)}
            onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => e.key === "Enter" && handleSearch()}
          className="max-w-[200px]"
        />
        <Button variant="outline" onClick={handleSearch}>
          搜索
        </Button>
      </div>

      {/* Config Table */}
      {isLoading ? (
        <Card>
          <CardContent className="p-6">
            <div className="space-y-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="h-10 animate-pulse rounded bg-muted" />
              ))}
            </div>
          </CardContent>
        </Card>
      ) : data && data.configs.length > 0 ? (
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Group</TableHead>
                  <TableHead>Key</TableHead>
                  <TableHead>格式</TableHead>
                  <TableHead>版本</TableHead>
                  <TableHead>更新时间</TableHead>
                  <TableHead>更新者</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.configs.map((cfg: typeof data.configs[number]) => (
                  <TableRow key={`${cfg.group}/${cfg.key}`}>
                    <TableCell>
                      <Badge variant="secondary">{cfg.group}</Badge>
                    </TableCell>
                    <TableCell>
                      <Link
                        to="/config/$group/$key"
                        params={{ group: cfg.group, key: cfg.key }}
                        className="font-medium text-primary hover:underline"
                      >
                        {cfg.key}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{formatLabels[cfg.format] || cfg.format}</Badge>
                    </TableCell>
                    <TableCell>v{cfg.version}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {new Date(cfg.updatedAt).toLocaleString("zh-CN")}
                    </TableCell>
                    <TableCell className="text-muted-foreground">{cfg.updatedBy}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12 text-center">
            <Settings className="mb-4 h-12 w-12 text-muted-foreground/50" />
            <p className="text-muted-foreground">暂无配置项</p>
            <Button variant="link" asChild className="mt-2">
              <Link to="/config/create">创建第一个配置</Link>
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Pagination */}
      {data && data.total > 0 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            共 {data.total} 条配置，第 {page} / {Math.ceil(data.total / pageSize)} 页
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
