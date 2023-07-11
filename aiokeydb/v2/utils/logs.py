from lazyops.utils.logs import get_logger, STATUS_COLOR, COLORED_MESSAGE_MAP, FALLBACK_STATUS_COLOR


class ColorMap:
    green: str = '\033[0;32m'
    red: str = '\033[0;31m'
    yellow: str = '\033[0;33m'
    blue: str = '\033[0;34m'
    magenta: str = '\033[0;35m'
    cyan: str = '\033[0;36m'
    white: str = '\033[0;37m'
    bold: str = '\033[1m'
    reset: str = '\033[0m'




class CustomizeLogger:

    @staticmethod
    def logger_formatter(record: dict) -> str:
        """
        To add a custom format for a module, add another `elif` clause with code to determine `extra` and `level`.

        From that module and all submodules, call logger with `logger.bind(foo='bar').info(msg)`.
        Then you can access it with `record['extra'].get('foo')`.
        """        
        extra = '<cyan>{name}</>:<cyan>{function}</>: '

        if record.get('extra'):
            if record['extra'].get('request_id'):
                extra = '<cyan>{name}</>:<cyan>{function}</>:<green>request_id: {extra[request_id]}</> '

            elif record['extra'].get('job_id') and record['extra'].get('queue_name') and record['extra'].get('kind'):
                status = record['extra'].get('status')
                color = STATUS_COLOR.get(status, FALLBACK_STATUS_COLOR)
                kind_color = STATUS_COLOR.get(record.get('extra', {}).get('kind'), FALLBACK_STATUS_COLOR)
                if not record['extra'].get('worker_name'):
                    record['extra']['worker_name'] = ''
                extra = '<cyan>{extra[queue_name]}</>:<bold><magenta>{extra[worker_name]}</></>:<bold><' + kind_color + '>{extra[kind]:<9}</></> <' + color + '>{extra[job_id]}</> '
                # if record['extra'].get('cid'):
                #     extra += '<green>({extra[cid]}/{extra[conn]})</> '

            elif record['extra'].get('kind') and record['extra'].get('queue_name'):
                if not record['extra'].get('worker_name'):
                    record['extra']['worker_name'] = ''
                kind_color = STATUS_COLOR.get(record.get('extra', {}).get('kind'), FALLBACK_STATUS_COLOR)
                extra = '<cyan>{extra[queue_name]}</>:<b><magenta>{extra[worker_name]}</></>:<b><' + kind_color + '>{extra[kind]:<9}</></> '
                # if record['extra'].get('cid'):
                #     extra += '<green>({extra[cid]}/{extra[conn]})</> '


        if 'result=tensor([' not in str(record['message']):
            return "<level>{level: <8}</> <green>{time:YYYY-MM-DD HH:mm:ss.SSS}</>: "\
                       + extra + "<level>{message}</level>\n"
        msg = str(record['message'])[:100].replace('{', '(').replace('}', ')')
        return "<level>{level: <8}</> <green>{time:YYYY-MM-DD HH:mm:ss.SSS}</>: "\
                   + extra + "<level>" + msg + f"</level>{STATUS_COLOR['reset']}\n"


logger = get_logger(
    format = CustomizeLogger.logger_formatter,
)
