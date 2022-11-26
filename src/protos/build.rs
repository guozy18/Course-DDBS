use std::error::Error;
use std::ffi::OsStr;
use std::fs;

const TARGET_DIR: &str = "src/protos_code_gen";

fn os_str_to_string(s: impl AsRef<OsStr>) -> String {
    s.as_ref().to_string_lossy().into_owned()
}

fn main() -> Result<(), Box<dyn Error>> {
    // Tells cargo to only rebuild if the proto file changed
    println!("cargo:rerun-if-changed=protos/");

    let protos_path = concat!(env!("CARGO_MANIFEST_DIR"), "/protos");

    let mut protos = fs::read_dir(protos_path)?
        .map(|e| e.unwrap())
        .filter(|entry| {
            let path = entry.path();
            let extension = path.extension().map(|x| x.to_str().unwrap());
            matches!(extension, Some("proto"))
        })
        .map(|entry| os_str_to_string(entry.file_name()))
        .collect::<Vec<_>>();
    protos.sort_unstable();
    dbg!(&protos);

    fs::create_dir_all(TARGET_DIR)?;
    tonic_build::configure()
        .out_dir(TARGET_DIR)
        .compile(&protos, &[protos_path.to_string()])
        .unwrap();

    let mut lib_rs = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(format!("{}/{}", TARGET_DIR, "mod.rs"))?;

    for proto in protos {
        let line = format!(
            "pub mod {};\n",
            std::path::Path::new(&proto)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
        );
        std::io::Write::write_all(&mut lib_rs, line.as_bytes())?;
    }

    Ok(())
}
