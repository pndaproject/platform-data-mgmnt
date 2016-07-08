node {

   stage 'Build'
   def workspace = pwd() 

   sh '''

      cd $PWD@script/data-service
      mvn versions:set -DnewVersion=$BRANCH_NAME
      mvn clean package

      cd ..

      cd $PWD/hdfs-cleaner
      mvn versions:set -DnewVersion=$BRANCH_NAME
      mvn clean package
	'''

   stage 'Test'
   sh '''
   '''

   stage 'Deploy' 
   build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "data-service"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}@script/data-service/target/data-service-${env.BRANCH_NAME}.tar.gz"]]
   build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "hdfs-cleaner"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}@script/hdfs-cleaner/target/hdfs-cleaner-${env.BRANCH_NAME}.tar.gz"]]


}