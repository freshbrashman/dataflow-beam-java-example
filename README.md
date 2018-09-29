# dataflow-beam-java-example

java版のDataflowをすぐに使えるようにするための雛形作成をしておくだけのもの。
beamのチュートリアルであるWordCountを元に作成する。
できるだけ最小の構成にしたいところ。

## 初期構築
https://beam.apache.org/get-started/quickstart-java/

初期構築時点では、beam 2.6.0

```
$ mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.6.0 \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false
```
