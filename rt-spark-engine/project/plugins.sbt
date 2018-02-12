/*resolvers += Resolver.url(
  "bintray-alpeb-sbt-plugins",
  url("http://dl.bintray.com/alpeb/sbt-plugins"))(
  Resolver.ivyStylePatterns)*/
// resolvers += "Local Maven Repository" at "file:///home/osboxes/.m2/repository"
logLevel := Level.Warn
//resolvers += Resolver.mavenLocal
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
