import { createFileRoute } from "@tanstack/react-router"
import { useQuery } from "@tanstack/react-query"
import { api } from "@/api/client"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Skeleton } from "@/components/ui/skeleton"
import { ArrowLeft, Activity } from "lucide-react"
import { Link } from "@tanstack/react-router"

export const Route = createFileRoute("/_app/registry/$serviceName")({
  component: ServiceDetailPage,
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

function ServiceDetailPage() {
  const { serviceName } = Route.useParams()

  const { data, isLoading } = useQuery({
    queryKey: ["registry", "service", serviceName],
    queryFn: () => api.registry.detail(serviceName),
  })

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-32 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (!data) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <p className="text-muted-foreground">服务未找到</p>
        <Button variant="link" asChild className="mt-2">
          <Link to="/registry">返回服务列表</Link>
        </Button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" asChild>
          <Link to="/registry">
            <ArrowLeft className="mr-1 h-4 w-4" />
            返回
          </Link>
        </Button>
        <span className="text-muted-foreground">/</span>
        <h1 className="text-2xl font-bold tracking-tight">{data.name}</h1>
      </div>

      {/* Overview */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5" />
            服务概览
          </CardTitle>
          <CardDescription>{data.name} 的基本信息与健康状态</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-4">
            <div>
              <p className="text-sm text-muted-foreground">服务名</p>
              <p className="font-medium">{data.name}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">标签</p>
              <div className="flex gap-1 flex-wrap mt-1">
                {data.tags.map((tag: string) => (
                  <Badge key={tag} variant="secondary" className="text-xs">{tag}</Badge>
                ))}
              </div>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">健康率</p>
              <p className="font-medium text-green-600 dark:text-green-400">{data.healthRate}%</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">实例数</p>
              <p className="font-medium">{data.instances.length}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Instances */}
      <Card>
        <CardHeader>
          <CardTitle>实例列表</CardTitle>
          <CardDescription>该服务的所有注册实例</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>实例 ID</TableHead>
                <TableHead>地址</TableHead>
                <TableHead>端口</TableHead>
                <TableHead>健康状态</TableHead>
                <TableHead>最后检查</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.instances.map((inst: typeof data.instances[number]) => (
                <TableRow key={inst.id}>
                  <TableCell className="font-mono text-xs">{inst.id}</TableCell>
                  <TableCell>{inst.address}</TableCell>
                  <TableCell>{inst.port}</TableCell>
                  <TableCell>
                    <Badge variant={statusColors[inst.status] || "default"}>
                      {statusLabels[inst.status] || inst.status}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(inst.lastCheck).toLocaleString("zh-CN")}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}
