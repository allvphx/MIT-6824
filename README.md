# MIT-6824

## MapReduce

The implementation for MapReduce.

[MapReduce Code](https://github.com/allvphx/MIT-6824/tree/2D-Pass/src/mr)

Points:
* Use broadcast to make it easier.

* Use `type` to rename some int types to make the code clearer and easier to maintain.

## Raft

The implementation for raft.

[Raft Code](https://github.com/allvphx/MIT-6824/blob/2D-Pass/src/raft/raft.go)

### 2A

Some interesting parts:
* The realization of concurrent sending of RPCs to other servers `sendVoteRequest`: 
  use `sync.Cond` to boardcast with a loop waiting, and maintain `remainPost` to prune the useless RPCs. (you can use a `for` loop to replace the `while` loop, shown in the code)

* Defining the state transformation of State Machine related algorithm as a function (In this code it is `TranState`) would helps a lot.

* Try to separate some parts, assign it into another goroutine would avoid lots of bugs.

* Print all the information you may use into the log when debug a distributed program, because the error may be hard to replay.

Some bugs that blocked me in this part:

* When trying to boardcast RPCs, you should make sure that **the Term is not changed** from the time you 
  send the first RPC to the time you use the recieved information to update the state machine .
    
* Not well coded `rf.getLog()` and `rf.isUpToDate`, be careful on these functions since it may have index `-1` , etc.

* The use of `WaitGroup` may incur lots of trouble since the `Add` may race with each others.

* Don't forget to reset the `Term` of an outdated Candidate.

* The `voteFor` should be set to `null` when receive a call from the Leader or a bigger Term.

### 2B

Some interesting parts:
* Using $2^x$ is a good way to decrease the `nextIndex[i]`, is quicker than the "skip term" method mentioned in the lecture.
However it is not friendly when you try to add the `SnapShot` in `2D`.
  
* The `applyLoop` makes it easier for you to update the `commitIndex`.

Some bugs that blocked me in this part:

* The Ping should be regarded as a normal AppendEntry, which means resending is needed.

* Be careful about the transformation between [0,...,n-1] and [1,...,n].

* Make sure to initialize all parameters in Args. For example, the code in the `trySendAppendEntry`.

* The `commmitIndex` is not changed continuously, and the `remainPost` may become -1.

### 2C, 2D

* Do not persist a temporary state!

* The `lastApplied` should be changed with `lastIncludeIndex`.

* Make sure to avoid the DeadLock in the following form.
```go
func (r *R) A() {
	r.mu.Lock()
	r.ch <- value
	r.mu.Unlock()
}

func (r *R) C() {
	r.mu.Lock() // deadlock here
	defer r.mu.Unlock()
	// the code here
}

func B() {
	<- r.ch
	C()
}
```