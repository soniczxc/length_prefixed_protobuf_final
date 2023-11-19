#ifndef SRC_PROTOBUF_PARSER_DELIMITEDMESSAGESSTREAMPARSER_HPP_
#define SRC_PROTOBUF_PARSER_DELIMITEDMESSAGESSTREAMPARSER_HPP_

#include "helpers.h"
#include <list>

using namespace std;

template <typename MessageType>
class DelimitedMessagesStreamParser {
private:
    std::vector<char> m_buffer;

public:
    typedef std::shared_ptr<const MessageType> PointerToConstValue;

    std::list<PointerToConstValue> parse(const std::string& data) {
        std::list<PointerToConstValue> list;
        size_t readedBytes{ 0 };
        std::copy(data.begin(), data.end(), std::back_inserter(m_buffer));
        while (m_buffer.size() > readedBytes) {
            std::shared_ptr<MessageType> msg = parseDelimited<MessageType>(&*m_buffer.begin(), m_buffer.size(), &readedBytes);
            if (msg){
                m_buffer.erase(m_buffer.begin(),m_buffer.begin() + (long long)readedBytes);
                list.push_back(msg);
                readedBytes = 0;
            }else{
                break;
            }
        }
        return list;
    };
};

#endif /* SRC_PROTOBUF_PARSER_DELIMITEDMESSAGESSTREAMPARSER_HPP_ */

