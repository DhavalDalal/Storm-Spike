package second

def Root = new XmlSlurper().parse('/Users/dhavald/Documents/workspace/Storm-Spike/resources/TestData.xml')

def allDataPoints = Root.DataPoint
println "allDataPoints = $allDataPoints"

allDataPoints.each { dataPoint ->
    println dataPoint.@source.text()
    println dataPoint.@Stream.text()
    println dataPoint.@Qualifier.text()
    println dataPoint.@DateTime.text()
    println dataPoint.@value.text()
}