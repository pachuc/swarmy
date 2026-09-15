use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::Command,
    sync::Arc,
};

use object_store::memory::InMemory;
use swarmy_core::{CHUNK_SIZE, ContentHash};
use swarmy_volume::{
    ChunkStore, Manifest,
    image::{Recipe, Source, build_ext4, upload_image},
};

#[tokio::test]
async fn image_ingestion_preserves_offsets_and_reuses_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("disk");
    let mut file = fs::File::create(&path).unwrap();
    file.write_all(&vec![42; CHUNK_SIZE as usize]).unwrap();
    file.write_all(&vec![0; CHUNK_SIZE as usize]).unwrap();
    file.write_all(&vec![42; CHUNK_SIZE as usize]).unwrap();
    let objects = Arc::new(InMemory::new());
    let first = upload_image(&path, objects.clone()).await.unwrap();
    assert_eq!(
        (
            first.chunks_total,
            first.chunks_stored,
            first.chunks_uploaded
        ),
        (3, 2, 1)
    );
    let second = upload_image(&path, objects.clone()).await.unwrap();
    assert_eq!(second.chunks_uploaded, 0);
    assert_eq!(first.header, second.header);
    let manifest = Manifest::load(&*objects, first.header).await.unwrap();
    for block in 0..3 {
        let hash = manifest.chunk_hash(&*objects, block).await.unwrap();
        assert_eq!(hash == ContentHash::ZERO, block == 1);
        let bytes = ChunkStore::new(objects.clone())
            .get_chunk(hash)
            .await
            .unwrap();
        assert!(
            bytes
                .iter()
                .all(|&byte| byte == if block == 1 { 0 } else { 42 })
        );
    }
    fs::write(&path, [1]).unwrap();
    assert!(upload_image(&path, objects).await.is_err());
}

#[test]
fn recipes_reject_typos_and_invalid_dimensions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recipe.toml");
    for text in [
        "disk_size = 0\nsource_date_epoch = 1\n[source]\nkind = 'directory'\npath = 'root'",
        "disk_size = 67108864\nsource_date_epoch = 1\n[source]\nkind = 'directory'\npath = 'root'\ntypo = true",
        "disk_size = 67108865\nsource_date_epoch = 1\n[source]\nkind = 'directory'\npath = 'root'",
    ] {
        fs::write(&path, text).unwrap();
        assert!(Recipe::load(&path).is_err());
    }
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../images/base-ubuntu");
    let (recipe, _, name) = Recipe::load(&base).unwrap();
    assert_eq!(name, "base-ubuntu");
    assert_eq!(recipe.disk_size, 8 * 1024 * 1024 * 1024);
}

struct Mount<'a>(&'a Path);
impl Drop for Mount<'_> {
    fn drop(&mut self) {
        assert!(
            Command::new("umount")
                .arg(self.0)
                .status()
                .unwrap()
                .success()
        );
    }
}

#[tokio::test]
async fn root_ext4_mount_round_trip_and_independent_rebuild() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping root ext4 integration test: not running as root");
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("seed");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("hello"), b"image fixture\n").unwrap();
    std::os::unix::fs::symlink("hello", root.join("link")).unwrap();
    let recipe = Recipe {
        disk_size: 64 * 1024 * 1024,
        source_date_epoch: 1_714_003_200,
        source: Source::Directory { path: root.clone() },
    };
    let first = build_ext4(&recipe, directory.path()).unwrap();
    let objects = Arc::new(InMemory::new());
    let uploaded = upload_image(first.path(), objects.clone()).await.unwrap();
    // Recreate the source to change host inode numbers and creation times.
    fs::remove_file(root.join("hello")).unwrap();
    fs::write(root.join("hello"), b"image fixture\n").unwrap();
    fs::File::open(root.join("hello"))
        .unwrap()
        .set_times(
            fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1234)),
        )
        .unwrap();
    let second = build_ext4(&recipe, directory.path()).unwrap();
    let repeated = upload_image(second.path(), objects).await.unwrap();
    assert_eq!(repeated.chunks_uploaded, 0);
    assert_eq!(uploaded.header, repeated.header);
    assert!(
        Command::new("e2fsck")
            .args(["-f", "-n"])
            .arg(first.path())
            .status()
            .unwrap()
            .success()
    );
    let mounted = directory.path().join("mounted");
    fs::create_dir(&mounted).unwrap();
    assert!(
        Command::new("mount")
            .args(["-o", "loop,ro,noload"])
            .arg(first.path())
            .arg(&mounted)
            .status()
            .unwrap()
            .success()
    );
    let _mount = Mount(&mounted);
    assert_eq!(fs::read(mounted.join("link")).unwrap(), b"image fixture\n");
    assert_eq!(
        fs::read_link(mounted.join("link")).unwrap(),
        Path::new("hello")
    );
    let mut file = fs::File::open(first.path()).unwrap();
    let mut magic = [0; 1082];
    file.read_exact(&mut magic).unwrap();
    assert_eq!(&magic[1080..], &[0x53, 0xef]);
}

#[test]
fn root_shell_recipe_runs_inside_copied_seed() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping shell recipe integration test: not running as root");
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let seed = directory.path().join("seed");
    fs::create_dir_all(seed.join("bin")).unwrap();
    fs::copy("/bin/sh", seed.join("bin/sh")).unwrap();
    let libraries = Command::new("ldd").arg("/bin/sh").output().unwrap();
    assert!(libraries.status.success());
    for library in String::from_utf8(libraries.stdout)
        .unwrap()
        .split_whitespace()
        .filter(|word| word.starts_with('/'))
    {
        let target = seed.join(library.trim_start_matches('/'));
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(library, target).unwrap();
    }
    fs::write(
        directory.path().join("build.sh"),
        "printf 'from chroot' > /created\n",
    )
    .unwrap();
    let recipe = Recipe {
        disk_size: 64 * 1024 * 1024,
        source_date_epoch: 1_714_003_200,
        source: Source::Shell {
            rootfs: "seed".into(),
            script: "build.sh".into(),
        },
    };
    let image = build_ext4(&recipe, directory.path()).unwrap();
    assert!(!seed.join("created").exists());
    let output = Command::new("debugfs")
        .args(["-R", "cat /created"])
        .arg(image.path())
        .output()
        .unwrap();
    assert_eq!(output.stdout, b"from chroot");
    fs::write(directory.path().join("build.sh"), "exit 17\n").unwrap();
    assert!(build_ext4(&recipe, directory.path()).is_err());
}

#[test]
fn root_oci_recipe_applies_layer_deletions() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping OCI recipe integration test: not running as root");
        return;
    }
    for tool in ["skopeo", "umoci"] {
        if Command::new(tool).arg("--version").output().is_err() {
            eprintln!("skipping OCI recipe integration test: {tool} is not installed");
            return;
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let layout = directory.path().join("layout");
    let bundle = directory.path().join("bundle");
    let reference = format!("{}:test", layout.display());
    assert!(
        Command::new("umoci")
            .args(["init", "--layout"])
            .arg(&layout)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("umoci")
            .args(["new", "--image", &reference])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("umoci")
            .args(["unpack", "--image", &reference])
            .arg(&bundle)
            .status()
            .unwrap()
            .success()
    );
    fs::write(bundle.join("rootfs/deleted"), "old layer").unwrap();
    repack(&reference, &bundle);
    fs::remove_file(bundle.join("rootfs/deleted")).unwrap();
    fs::write(bundle.join("rootfs/kept"), "new layer").unwrap();
    repack(&reference, &bundle);
    let recipe = Recipe {
        disk_size: 64 * 1024 * 1024,
        source_date_epoch: 1_714_003_200,
        source: Source::Oci {
            reference: format!("oci:{reference}"),
        },
    };
    let image = build_ext4(&recipe, directory.path()).unwrap();
    let mounted = directory.path().join("mounted");
    fs::create_dir(&mounted).unwrap();
    assert!(
        Command::new("mount")
            .args(["-o", "loop,ro,noload"])
            .arg(image.path())
            .arg(&mounted)
            .status()
            .unwrap()
            .success()
    );
    let _mount = Mount(&mounted);
    assert_eq!(fs::read(mounted.join("kept")).unwrap(), b"new layer");
    assert!(!mounted.join("deleted").exists());
}

fn repack(reference: &str, bundle: &Path) {
    assert!(
        Command::new("umoci")
            .args(["repack", "--image", reference])
            .arg(bundle)
            .status()
            .unwrap()
            .success()
    );
}
