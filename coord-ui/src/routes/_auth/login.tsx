import { createFileRoute } from "@tanstack/react-router"
import { z } from "zod"
import { useState } from "react"
import { useForm } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { useLogin } from "@/hooks/useAuth"
import { Eye, EyeOff, Shield } from "lucide-react"

const loginSchema = z.object({
  roleId: z.string().min(1, "请输入 Role ID"),
  secretId: z.string().min(1, "请输入 Secret ID"),
})

type LoginForm = z.infer<typeof loginSchema>

const searchSchema = z.object({
  redirect: z.string().optional(),
})

export const Route = createFileRoute("/_auth/login")({
  validateSearch: searchSchema,
  component: LoginPage,
})

function LoginPage() {
  const [showSecret, setShowSecret] = useState(false)
  const login = useLogin()

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<LoginForm>({
    resolver: zodResolver(loginSchema),
  })

  const onSubmit = (data: LoginForm) => {
    login.mutate(data)
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-muted/50 p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-1 text-center">
          <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-primary/10">
            <Shield className="h-6 w-6 text-primary" />
          </div>
          <CardTitle className="text-2xl">Coord 控制台</CardTitle>
          <CardDescription>使用管理员分发的 AppRole 凭据登录</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="roleId">Role ID</Label>
              <Input
                id="roleId"
                placeholder="请输入 Role ID"
                {...register("roleId")}
                aria-invalid={!!errors.roleId}
              />
              {errors.roleId && (
                <p className="text-sm text-destructive">{errors.roleId.message}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="secretId">Secret ID</Label>
              <div className="relative">
                <Input
                  id="secretId"
                  type={showSecret ? "text" : "password"}
                  placeholder="请输入 Secret ID"
                  {...register("secretId")}
                  aria-invalid={!!errors.secretId}
                  className="pr-10"
                />
                <button
                  type="button"
                  onClick={() => setShowSecret(!showSecret)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"
                  aria-label={showSecret ? "隐藏 Secret ID" : "显示 Secret ID"}
                >
                  {showSecret ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
              {errors.secretId && (
                <p className="text-sm text-destructive">{errors.secretId.message}</p>
              )}
            </div>

            {login.isError && (
              <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
                {login.error instanceof Error ? login.error.message : "登录失败，请检查凭据"}
              </div>
            )}

            <Button type="submit" className="w-full" disabled={login.isPending}>
              {login.isPending ? "登录中..." : "登录"}
            </Button>
          </form>

          <p className="mt-4 text-center text-xs text-muted-foreground">
            凭据由管理员通过 Coord CLI 分发，如未获取请联系管理员
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
