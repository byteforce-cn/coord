;; ===========================================================================
;; ~/.lein/profiles.clj — domestic (China) mirrors for Leiningen
;; Applied to every lein project on this node (including jepsen).
;;
;; The mirror URLs are declared as :repositories (replacing the default
;; "central"/"clojars" entries) instead of :mirrors because lein does not
;; reliably apply :mirrors during plugin resolution — :repositories apply to
;; both normal dependencies AND plugins.
;;
;;   Maven Central -> Aliyun public   https://maven.aliyun.com/repository/public
;;   Clojars       -> Tencent mirror  https://mirrors.cloud.tencent.com/nexus/repository/clojars
;;   fallback      -> direct repo.clojars.org (extra repository, tried last)
;; ===========================================================================
{:user {:repositories
        {"central"        {:url "https://maven.aliyun.com/repository/public"}
         "clojars"        {:url "https://mirrors.cloud.tencent.com/nexus/repository/clojars"}
         "clojars-direct" {:url "https://repo.clojars.org/"}}}}
