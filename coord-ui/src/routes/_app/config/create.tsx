import { createFileRoute } from "@tanstack/react-router"
import { useForm } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { z } from "zod"
import { useMutation } from "@tanstack/react-query"
import { api } from "@/api/client"
import { useNavigate } from "@tanstack/react-router"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { ArrowLeft } from "lucide-react"
import { Link } from "@tanstack/react-router"

const createConfigSchema = z.object({
  group: z.string().min(1, "请输入 Group"),
  key: z.string().min(1, "请输入 Key"),
  format: z.enum(["yaml", "json", "toml", "text"]),
  data: z.string().min(1, "请输入配置内容"),
  changeNote: z.string().optional(),
})

type CreateConfigForm = z.infer<typeof createConfigSchema>

export const Route = createFileRoute("/_app/config/create")({
  component: CreateConfigPage,
})

function CreateConfigPage() {
  const navigate = useNavigate()

  const {
    register,
    handleSubmit,
    setValue,
    watch,
    formState: { errors },
  } = useForm<CreateConfigForm>({
    resolver: zodResolver(createConfigSchema),
    defaultValues: { format: "yaml" },
  })

  const format = watch("format")

  const createMutation = useMutation({
    mutationFn: (data: CreateConfigForm) => api.config.create(data),
    onSuccess: (_, vars) => {
      navigate({
        to: "/config/$group/$key",
        params: { group: vars.group, key: vars.key },
      })
    },
  })

  const onSubmit = (data: CreateConfigForm) => {
    createMutation.mutate(data)
  }

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" asChild>
          <Link to="/config">
            <ArrowLeft className="mr-1 h-4 w-4" />
            返回
          </Link>
        </Button>
        <h1 className="text-2xl font-bold tracking-tight">新建配置</h1>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>配置信息</CardTitle>
          <CardDescription>创建新的配置项，配置将以指定格式存储</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="group">Group</Label>
                <Input id="group" placeholder="例如: app" {...register("group")} />
                {errors.group && <p className="text-sm text-destructive">{errors.group.message}</p>}
              </div>
              <div className="space-y-2">
                <Label htmlFor="key">Key</Label>
                <Input id="key" placeholder="例如: database" {...register("key")} />
                {errors.key && <p className="text-sm text-destructive">{errors.key.message}</p>}
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="format">格式</Label>
              <Select value={format} onValueChange={(v) => setValue("format", v as typeof format)}>
                <SelectTrigger>
                  <SelectValue placeholder="选择格式" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="yaml">YAML</SelectItem>
                  <SelectItem value="json">JSON</SelectItem>
                  <SelectItem value="toml">TOML</SelectItem>
                  <SelectItem value="text">TEXT</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="data">配置内容</Label>
              <Textarea
                id="data"
                rows={12}
                placeholder="输入配置内容..."
                className="font-mono text-sm"
                {...register("data")}
              />
              {errors.data && <p className="text-sm text-destructive">{errors.data.message}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="changeNote">变更说明（可选）</Label>
              <Input id="changeNote" placeholder="简要描述变更内容" {...register("changeNote")} />
            </div>

            {createMutation.isError && (
              <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
                {createMutation.error instanceof Error ? createMutation.error.message : "创建失败"}
              </div>
            )}

            <div className="flex justify-end gap-3">
              <Button variant="outline" type="button" asChild>
                <Link to="/config">取消</Link>
              </Button>
              <Button type="submit" disabled={createMutation.isPending}>
                {createMutation.isPending ? "创建中..." : "创建配置"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
