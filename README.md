pre-Build:
git clone https://github.com/docker/distribution/
git clone https://github.com/ziranwei/upyun-registry.git distribution/registry/storage/driver/upyun-registry/

build:
import _ "github.com/docker/distribution/registry/storage/driver/upyun-registry" in file: distribution/cmd/registry/main.go

configure:
edit config-dev.yml
storage:
    upyun:
        username: yejingx
        password: yejingx123
        bucket: yejingx
        rootdirectory: /var00/lib/registry/
    
