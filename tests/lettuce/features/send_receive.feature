Feature: Send/Receive to Kafka

  @basic
  Scenario: Send data from filename to topic and read it back
    given a known initial offset
    when we load 8 records from the source file
    and we read from the initial offset
    then we register no errors
    and we receive the same number of records
    and the records retrieved are identical to those sent

  @checkpoint
  Scenario: Consumed data is committed at checkpoints correctly
    given the known initial offset
    and we load 8 records from the source file
    when we read from the initial offset with a checkpoint frequency of 2
    then we register no checkpoint errors
    and we have the correct number of successful checkpoints


  @checkpoint2
  Scenario: Consumed data is committed at checkpoints correctly
    given the known initial offset
    and we load 7 records from the source file
    when we read from the initial offset with a checkpoint frequency of 2
    then we register no checkpoint errors
    and we have the correct number of successful checkpoints