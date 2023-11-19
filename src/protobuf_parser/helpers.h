#ifndef SRC_PROTOBUF_PARSER_HELPERS_H_
#define SRC_PROTOBUF_PARSER_HELPERS_H_

#include <iostream>
#include <vector>
#include <fstream>
#include "message.pb.h"

using Data = std::vector<char>;
using PointerToData = std::shared_ptr<Data>;
using PointerToConstData = std::shared_ptr<const Data>;
using uint8 = google::protobuf::uint8;
using uint32 = google::protobuf::uint32;
using WrapperMessage = TestTask::Messages::WrapperMessage;
using namespace google::protobuf::io;

template<typename Message>
std::shared_ptr<Message> parseDelimited(const void* data, size_t size, size_t* bytesConsumed = nullptr) {
/*!
 * \brief Расшифровывает сообщение, предваренное длиной из массива байтов.
 *
 * \tparam Message Тип сообщения, для работы с которым предназначена данная
 * функция.
 *
 * \param data Указатель на буфер данных.
 * \param size Размер буфера данных.
 * \param bytesConsumed Количество байт, которое потребовалось для расшифровки
 * сообщения в случае успеха.
 *
 * \return Умный указатель на сообщение. Если удалось расшифровать сообщение, то
 * он не пустой.
 */
    std::shared_ptr<Message> message {std::make_shared<Message>(Message())};
    uint32 message_size {0};

    CodedInputStream codedInput((uint8*)data, size);

    if (!codedInput.ReadVarint32(&message_size))
        return nullptr;

    CodedInputStream::Limit limit = codedInput.PushLimit(message_size);
    const size_t varintSize = CodedOutputStream::VarintSize32(message_size);
    const size_t totalFrameSize = varintSize + message_size;

    if (size < totalFrameSize)
        return nullptr;

    if (!message->ParseFromCodedStream(&codedInput)){
        throw std::runtime_error("Wrong Data");
    }else{
        *bytesConsumed = totalFrameSize;
    }

    codedInput.PopLimit(limit);

    return message;
};

template <typename Message>
PointerToConstData serializeDelimited(const Message& msg){
    const size_t messageSize = msg.ByteSizeLong();
    const size_t headerSize = google::protobuf::io::CodedOutputStream::VarintSize32(messageSize);

    const PointerToData& result = std::make_shared<Data>(headerSize + messageSize);

    google::protobuf::uint8* buffer = reinterpret_cast<google::protobuf::uint8*>(&*result->begin());

    google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(messageSize, buffer);
    msg.SerializeToArray(buffer + headerSize, messageSize);

    return result;
};

WrapperMessage* create_fast_response(std::string date) {
    WrapperMessage* message;

    try {
        message = new WrapperMessage();
    } catch (std::bad_alloc& ex) {
        std::cout << "Caught bad_alloc: " << ex.what() << std::endl;
    }

    message->mutable_fast_response()
            ->set_current_date_time(date);
    return message;
};
WrapperMessage* create_slow_response(unsigned count) {
    WrapperMessage* message;

    try {
        message = new WrapperMessage();
    } catch (std::bad_alloc& ex) {
        std::cout << "Caught bad_alloc: " << ex.what() << std::endl;
    }

    message->mutable_slow_response()
            ->set_connected_client_count(count);
    return message;
};
WrapperMessage* create_request_for_fast_response() {
    WrapperMessage* message;
    try {
        message = new WrapperMessage();
    } catch (std::bad_alloc& ex) {
        std::cout << "Caught bad_alloc: " << ex.what() << std::endl;
    }

    *message->mutable_request_for_fast_response() = TestTask::Messages::RequestForFastResponse();
    return message;
};
WrapperMessage* create_request_for_slow_response(unsigned long time) {
    WrapperMessage* message;
    try {
        message = new WrapperMessage();
    } catch (std::bad_alloc& ex) {
        std::cout << "Caught bad_alloc: " << ex.what() << std::endl;
    }

    message->mutable_request_for_slow_response()
            ->set_time_in_seconds_to_sleep(time);
    return message;
};

#endif /* SRC_PROTOBUF_PARSER_HELPERS_H_ */
