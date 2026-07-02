import { createFileRoute, useNavigate } from "@tanstack/react-router"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { api } from "@/api/client"
import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { ArrowLeft, History, RotateCcw, Save, Trash2 } from "lucide-react"
import { Link } from "@tanstack/react-router"

export const Route = createFileRoute("/_app/config/$group/$key")({
  component: ConfigDetailPage,
})

function ConfigDetailPage() {
  const { group, key } = Route.useParams()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [isEditing, setIsEditing] = useState(false)
  const [editData, setEditData] = useState("")
  const [editNote, setEditNote] = useState("")
  const [showVersions, setShowVersions] = useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ["config", "detail", group, key],
    queryFn: () => api.config.detail(group, key),
  })

  const { data: versions } = useQuery({
    queryKey: ["config", "versions", group, key],
    queryFn: () => api.config.versions(group, key),
    enabled: showVersions,
  })

  const updateMutation = useMutation({
    mutationFn: (body: { data: string; version: number; changeNote?: string }) =>
      api.config.update(group, key, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["config", "detail", group, key] })
      queryClient.invalidateQueries({ queryKey: ["config", "versions", group, key] })
      setIsEditing(false)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: () => api.config.delete(group, key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["config", "list"] })
      navigate({ to: "/config" })
    },
  })

  const rollbackMutation = useMutation({
    mutationFn: (version: number) => api.config.rollback(group, key, version),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["config", "detail", group, key] })
      queryClient.invalidateQueries({ queryKey: ["config", "versions", group, key] })
    },
  })

  const formatLabels: Record<string, string> = {
    yaml: "YAML", json: "JSON", toml: "TOML", text: "TEXT",
  }

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
        <p className="text-muted-foreground">配置未找到</p>
        <Button variant="link" asChild className="mt-2">
          <Link to="/config">返回配置列表</Link>
        </Button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" asChild>
          <Link to="/config">
            <ArrowLeft className="mr-1 h-4 w-4" />
            返回
          </Link>
        </Button>
        <span className="text-muted-foreground">/</span>
        <Badge variant="secondary">{data.group}</Badge>
        <span className="text-muted-foreground">/</span>
        <h1 className="text-2xl font-bold tracking-tight">{data.key}</h1>
      </div>

      {/* Meta */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-lg">配置元数据</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Group:</span>{" "}
              <Badge variant="secondary">{data.group}</Badge>
            </div>
            <div>
              <span className="text-muted-foreground">Key:</span>{" "}
              <span className="font-medium">{data.key}</span>
            </div>
            <div>
              <span className="text-muted-foreground">格式:</span>{" "}
              <Badge variant="outline">{formatLabels[data.format] || data.format}</Badge>
            </div>
            <div>
              <span className="text-muted-foreground">版本:</span>{" "}
              <span className="font-medium">v{data.version}</span>
            </div>
            <div>
              <span className="text-muted-foreground">更新者:</span>{" "}
              <span className="font-medium">{data.updatedBy}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Content */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle className="text-lg">配置内容</CardTitle>
            <CardDescription>
              最后更新: {new Date(data.updatedAt).toLocaleString("zh-CN")}
              {data.changeNote && ` — ${data.changeNote}`}
            </CardDescription>
          </div>
          <div className="flex gap-2">
            {!isEditing ? (
              <>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setEditData(data.data)
                    setIsEditing(true)
                  }}
                >
                  <Save className="mr-1 h-4 w-4" />
                  编辑
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    if (confirm("确定删除此配置吗？所有历史版本将被永久删除。")) {
                      deleteMutation.mutate()
                    }
                  }}
                  disabled={deleteMutation.isPending}
                >
                  <Trash2 className="mr-1 h-4 w-4 text-destructive" />
                  删除
                </Button>
              </>
            ) : (
              <>
                <Button variant="outline" size="sm" onClick={() => setIsEditing(false)}>
                  取消
                </Button>
                <Button
                  size="sm"
                  onClick={() =>
                    updateMutation.mutate({
                      data: editData,
                      version: data.version,
                      changeNote: editNote || undefined,
                    })
                  }
                  disabled={updateMutation.isPending}
                >
                  {updateMutation.isPending ? "保存中..." : "保存"}
                </Button>
              </>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {isEditing ? (
            <div className="space-y-4">
              <Textarea
                value={editData}
                onChange={(e) => setEditData(e.target.value)}
                rows={15}
                className="font-mono text-sm"
              />
              <div className="space-y-2">
                <Label htmlFor="editNote">变更说明</Label>
                <Input
                  id="editNote"
                  value={editNote}
                  onChange={(e) => setEditNote(e.target.value)}
                  placeholder="简要描述变更内容"
                />
              </div>
              {updateMutation.isError && (
                <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
                  {updateMutation.error instanceof Error
                    ? updateMutation.error.message
                    : "更新失败，可能存在并发冲突（CAS 失败）"}
                </div>
              )}
            </div>
          ) : (
            <pre className="rounded-md bg-muted p-4 text-sm font-mono overflow-x-auto max-h-96 overflow-y-auto">
              {data.data}
            </pre>
          )}
        </CardContent>
      </Card>

      {/* Version History */}
      <Card>
        <CardHeader
          className="flex flex-row items-center justify-between cursor-pointer"
          onClick={() => setShowVersions(!showVersions)}
        >
          <div className="flex items-center gap-2">
            <History className="h-5 w-5" />
            <CardTitle className="text-lg">历史版本</CardTitle>
          </div>
          <Button variant="ghost" size="sm">
            {showVersions ? "收起" : "展开"}
          </Button>
        </CardHeader>
        {showVersions && (
          <CardContent>
            {versions && versions.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>版本</TableHead>
                    <TableHead>更新时间</TableHead>
                    <TableHead>更新者</TableHead>
                    <TableHead>变更说明</TableHead>
                    <TableHead>操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {versions.map((v) => (
                    <TableRow key={v.version}>
                      <TableCell className="font-mono">v{v.version}</TableCell>
                      <TableCell>{new Date(v.updatedAt).toLocaleString("zh-CN")}</TableCell>
                      <TableCell>{v.updatedBy}</TableCell>
                      <TableCell className="max-w-[200px] truncate">{v.changeNote || "-"}</TableCell>
                      <TableCell>
                        {v.version !== data.version && (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              if (confirm(`确定回滚到版本 v${v.version} 吗？将生成新版本。`)) {
                                rollbackMutation.mutate(v.version)
                              }
                            }}
                            disabled={rollbackMutation.isPending}
                          >
                            <RotateCcw className="mr-1 h-3 w-3" />
                            回滚
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="text-center text-muted-foreground py-4">暂无历史版本</p>
            )}
          </CardContent>
        )}
      </Card>
    </div>
  )
}
