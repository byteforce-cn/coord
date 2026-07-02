// coord-macros: 过程宏 Crate
//
// 提供派生宏等编译期代码生成能力，简化 coord-core 类型的使用。
//
// 宏列表：
// - #[derive(ValidateRevision)] — 为包含 revision: u64 字段的结构体生成验证方法

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields};

/// 为结构体生成 revision 验证方法。
///
/// 要求结构体包含 `revision: u64` 字段。生成以下方法：
/// - `fn validate_revision(&self) -> Result<(), Error>` — 验证 revision > 0
///
/// # 示例
/// ```ignore
/// #[derive(ValidateRevision)]
/// struct PutResponse {
///     revision: u64,
///     // ...其他字段
/// }
/// ```
#[proc_macro_derive(ValidateRevision)]
pub fn derive_validate_revision(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // 检查是否包含 revision 字段
    let has_revision = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields.named.iter().any(|f| {
                f.ident.as_ref().map_or(false, |id| id == "revision")
            }),
            _ => false,
        },
        _ => false,
    };

    if !has_revision {
        return syn::Error::new_spanned(
            &input,
            "#[derive(ValidateRevision)] requires a field named `revision` of type `u64`",
        )
        .to_compile_error()
        .into();
    }

    let expanded = quote! {
        impl #name {
            /// 验证 revision 字段合法（> 0）。
            ///
            /// # 错误
            /// 返回 `Error::InvalidArgument` 当 revision == 0。
            pub fn validate_revision(&self) -> ::coord_core::error::Result<()> {
                if self.revision == 0 {
                    return Err(::coord_core::error::Error::InvalidArgument(
                        "revision must be > 0".into()
                    ));
                }
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

/// 为结构体生成 Builder 模式构造器。
///
/// 为每个字段生成 `with_{field}(self, value) -> Self` 方法。
///
/// # 示例
/// ```ignore
/// #[derive(Builder)]
/// struct Config {
///     timeout_ms: u64,
///     max_retries: u32,
///     name: String,
/// }
/// ```
#[proc_macro_derive(Builder)]
pub fn derive_builder(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "#[derive(Builder)] only supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input,
                "#[derive(Builder)] only supports structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let setters: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;
            let method_name = quote::format_ident!("with_{}", field_name);
            quote! {
                pub fn #method_name(mut self, value: #field_type) -> Self {
                    self.#field_name = value;
                    self
                }
            }
        })
        .collect();

    let expanded = quote! {
        impl #name {
            #(#setters)*
        }
    };

    TokenStream::from(expanded)
}


