use proc_macro::TokenStream;
use quote::quote;
use syn::ItemFn;

#[proc_macro_attribute]
pub fn elapsed(_attr: TokenStream, function_def: TokenStream) -> TokenStream {
    let mut item = syn::parse(function_def).unwrap();
    let fn_item = match &mut item {
        syn::Item::Fn(fn_item) => fn_item,
        _ => panic!("only support function now"),
    };
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = fn_item;
    let function_body = block.clone();
    let fn_name = sig.ident.clone();
    let log_time = format!("execute {} tooks {{:?}}", fn_name);
    let new_function_def = quote! {
        #(#attrs)* #vis #sig {
            let start_for_elapsed_macro = std::time::Instant::now();
            let res = #function_body;
            trace!(#log_time, start_for_elapsed_macro.elapsed());
            res
        }
    };
    TokenStream::from(new_function_def)
}
