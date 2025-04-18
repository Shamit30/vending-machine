/**
 * Autogenerated by Thrift Compiler (0.21.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "BeveragePreferenceService.h"

namespace vending_machine {


BeveragePreferenceService_GetBeverage_args::~BeveragePreferenceService_GetBeverage_args() noexcept {
}

BeveragePreferenceService_GetBeverage_args::BeveragePreferenceService_GetBeverage_args() noexcept
   : btype(static_cast<BeverageType::type>(0)) {
}

uint32_t BeveragePreferenceService_GetBeverage_args::read(::apache::thrift::protocol::TProtocol* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast8;
          xfer += iprot->readI32(ecast8);
          this->btype = static_cast<BeverageType::type>(ecast8);
          this->__isset.btype = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t BeveragePreferenceService_GetBeverage_args::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("BeveragePreferenceService_GetBeverage_args");

  xfer += oprot->writeFieldBegin("btype", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32(static_cast<int32_t>(this->btype));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


BeveragePreferenceService_GetBeverage_pargs::~BeveragePreferenceService_GetBeverage_pargs() noexcept {
}


uint32_t BeveragePreferenceService_GetBeverage_pargs::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("BeveragePreferenceService_GetBeverage_pargs");

  xfer += oprot->writeFieldBegin("btype", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32(static_cast<int32_t>((*(this->btype))));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


BeveragePreferenceService_GetBeverage_result::~BeveragePreferenceService_GetBeverage_result() noexcept {
}

BeveragePreferenceService_GetBeverage_result::BeveragePreferenceService_GetBeverage_result() noexcept
   : success() {
}

uint32_t BeveragePreferenceService_GetBeverage_result::read(::apache::thrift::protocol::TProtocol* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 0:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->success);
          this->__isset.success = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->se.read(iprot);
          this->__isset.se = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t BeveragePreferenceService_GetBeverage_result::write(::apache::thrift::protocol::TProtocol* oprot) const {

  uint32_t xfer = 0;

  xfer += oprot->writeStructBegin("BeveragePreferenceService_GetBeverage_result");

  if (this->__isset.success) {
    xfer += oprot->writeFieldBegin("success", ::apache::thrift::protocol::T_STRING, 0);
    xfer += oprot->writeString(this->success);
    xfer += oprot->writeFieldEnd();
  } else if (this->__isset.se) {
    xfer += oprot->writeFieldBegin("se", ::apache::thrift::protocol::T_STRUCT, 1);
    xfer += this->se.write(oprot);
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


BeveragePreferenceService_GetBeverage_presult::~BeveragePreferenceService_GetBeverage_presult() noexcept {
}


uint32_t BeveragePreferenceService_GetBeverage_presult::read(::apache::thrift::protocol::TProtocol* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 0:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString((*(this->success)));
          this->__isset.success = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->se.read(iprot);
          this->__isset.se = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

void BeveragePreferenceServiceClient::GetBeverage(std::string& _return, const BeverageType::type btype)
{
  send_GetBeverage(btype);
  recv_GetBeverage(_return);
}

void BeveragePreferenceServiceClient::send_GetBeverage(const BeverageType::type btype)
{
  int32_t cseqid = 0;
  oprot_->writeMessageBegin("GetBeverage", ::apache::thrift::protocol::T_CALL, cseqid);

  BeveragePreferenceService_GetBeverage_pargs args;
  args.btype = &btype;
  args.write(oprot_);

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush();
}

void BeveragePreferenceServiceClient::recv_GetBeverage(std::string& _return)
{

  int32_t rseqid = 0;
  std::string fname;
  ::apache::thrift::protocol::TMessageType mtype;

  iprot_->readMessageBegin(fname, mtype, rseqid);
  if (mtype == ::apache::thrift::protocol::T_EXCEPTION) {
    ::apache::thrift::TApplicationException x;
    x.read(iprot_);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
    throw x;
  }
  if (mtype != ::apache::thrift::protocol::T_REPLY) {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }
  if (fname.compare("GetBeverage") != 0) {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }
  BeveragePreferenceService_GetBeverage_presult result;
  result.success = &_return;
  result.read(iprot_);
  iprot_->readMessageEnd();
  iprot_->getTransport()->readEnd();

  if (result.__isset.success) {
    // _return pointer has now been filled
    return;
  }
  if (result.__isset.se) {
    throw result.se;
  }
  throw ::apache::thrift::TApplicationException(::apache::thrift::TApplicationException::MISSING_RESULT, "GetBeverage failed: unknown result");
}

bool BeveragePreferenceServiceProcessor::dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext) {
  ProcessMap::iterator pfn;
  pfn = processMap_.find(fname);
  if (pfn == processMap_.end()) {
    iprot->skip(::apache::thrift::protocol::T_STRUCT);
    iprot->readMessageEnd();
    iprot->getTransport()->readEnd();
    ::apache::thrift::TApplicationException x(::apache::thrift::TApplicationException::UNKNOWN_METHOD, "Invalid method name: '"+fname+"'");
    oprot->writeMessageBegin(fname, ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return true;
  }
  (this->*(pfn->second))(seqid, iprot, oprot, callContext);
  return true;
}

void BeveragePreferenceServiceProcessor::process_GetBeverage(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext)
{
  void* ctx = nullptr;
  if (this->eventHandler_.get() != nullptr) {
    ctx = this->eventHandler_->getContext("BeveragePreferenceService.GetBeverage", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "BeveragePreferenceService.GetBeverage");

  if (this->eventHandler_.get() != nullptr) {
    this->eventHandler_->preRead(ctx, "BeveragePreferenceService.GetBeverage");
  }

  BeveragePreferenceService_GetBeverage_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != nullptr) {
    this->eventHandler_->postRead(ctx, "BeveragePreferenceService.GetBeverage", bytes);
  }

  BeveragePreferenceService_GetBeverage_result result;
  try {
    iface_->GetBeverage(result.success, args.btype);
    result.__isset.success = true;
  } catch (ServiceException &se) {
    result.se = std::move(se);
    result.__isset.se = true;
  } catch (const std::exception& e) {
    if (this->eventHandler_.get() != nullptr) {
      this->eventHandler_->handlerError(ctx, "BeveragePreferenceService.GetBeverage");
    }

    ::apache::thrift::TApplicationException x(e.what());
    oprot->writeMessageBegin("GetBeverage", ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return;
  }

  if (this->eventHandler_.get() != nullptr) {
    this->eventHandler_->preWrite(ctx, "BeveragePreferenceService.GetBeverage");
  }

  oprot->writeMessageBegin("GetBeverage", ::apache::thrift::protocol::T_REPLY, seqid);
  result.write(oprot);
  oprot->writeMessageEnd();
  bytes = oprot->getTransport()->writeEnd();
  oprot->getTransport()->flush();

  if (this->eventHandler_.get() != nullptr) {
    this->eventHandler_->postWrite(ctx, "BeveragePreferenceService.GetBeverage", bytes);
  }
}

::std::shared_ptr< ::apache::thrift::TProcessor > BeveragePreferenceServiceProcessorFactory::getProcessor(const ::apache::thrift::TConnectionInfo& connInfo) {
  ::apache::thrift::ReleaseHandler< BeveragePreferenceServiceIfFactory > cleanup(handlerFactory_);
  ::std::shared_ptr< BeveragePreferenceServiceIf > handler(handlerFactory_->getHandler(connInfo), cleanup);
  ::std::shared_ptr< ::apache::thrift::TProcessor > processor(new BeveragePreferenceServiceProcessor(handler));
  return processor;
}

void BeveragePreferenceServiceConcurrentClient::GetBeverage(std::string& _return, const BeverageType::type btype)
{
  int32_t seqid = send_GetBeverage(btype);
  recv_GetBeverage(_return, seqid);
}

int32_t BeveragePreferenceServiceConcurrentClient::send_GetBeverage(const BeverageType::type btype)
{
  int32_t cseqid = this->sync_->generateSeqId();
  ::apache::thrift::async::TConcurrentSendSentry sentry(this->sync_.get());
  oprot_->writeMessageBegin("GetBeverage", ::apache::thrift::protocol::T_CALL, cseqid);

  BeveragePreferenceService_GetBeverage_pargs args;
  args.btype = &btype;
  args.write(oprot_);

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush();

  sentry.commit();
  return cseqid;
}

void BeveragePreferenceServiceConcurrentClient::recv_GetBeverage(std::string& _return, const int32_t seqid)
{

  int32_t rseqid = 0;
  std::string fname;
  ::apache::thrift::protocol::TMessageType mtype;

  // the read mutex gets dropped and reacquired as part of waitForWork()
  // The destructor of this sentry wakes up other clients
  ::apache::thrift::async::TConcurrentRecvSentry sentry(this->sync_.get(), seqid);

  while(true) {
    if(!this->sync_->getPending(fname, mtype, rseqid)) {
      iprot_->readMessageBegin(fname, mtype, rseqid);
    }
    if(seqid == rseqid) {
      if (mtype == ::apache::thrift::protocol::T_EXCEPTION) {
        ::apache::thrift::TApplicationException x;
        x.read(iprot_);
        iprot_->readMessageEnd();
        iprot_->getTransport()->readEnd();
        sentry.commit();
        throw x;
      }
      if (mtype != ::apache::thrift::protocol::T_REPLY) {
        iprot_->skip(::apache::thrift::protocol::T_STRUCT);
        iprot_->readMessageEnd();
        iprot_->getTransport()->readEnd();
      }
      if (fname.compare("GetBeverage") != 0) {
        iprot_->skip(::apache::thrift::protocol::T_STRUCT);
        iprot_->readMessageEnd();
        iprot_->getTransport()->readEnd();

        // in a bad state, don't commit
        using ::apache::thrift::protocol::TProtocolException;
        throw TProtocolException(TProtocolException::INVALID_DATA);
      }
      BeveragePreferenceService_GetBeverage_presult result;
      result.success = &_return;
      result.read(iprot_);
      iprot_->readMessageEnd();
      iprot_->getTransport()->readEnd();

      if (result.__isset.success) {
        // _return pointer has now been filled
        sentry.commit();
        return;
      }
      if (result.__isset.se) {
        sentry.commit();
        throw result.se;
      }
      // in a bad state, don't commit
      throw ::apache::thrift::TApplicationException(::apache::thrift::TApplicationException::MISSING_RESULT, "GetBeverage failed: unknown result");
    }
    // seqid != rseqid
    this->sync_->updatePending(fname, mtype, rseqid);

    // this will temporarily unlock the readMutex, and let other clients get work done
    this->sync_->waitForWork(seqid);
  } // end while(true)
}

} // namespace

