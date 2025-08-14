from flask import Flask, request, render_template, jsonify, flash, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import pymysql
import requests
from datetime import datetime

app = Flask(__name__)
app.secret_key = '111'

# —— Flask-Login 配置 —— #
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'   # 未登录访问受保护页面跳转到 login 视图

# 管理员账号信息（生产请从数据库读取）
admins = {
    'xxc': generate_password_hash('Xxc1203,./')  # 账号：admin，密码：admin123
}

class User(UserMixin):
    def __init__(self, username):
        self.id = username

@login_manager.user_loader
def load_user(user_id):
    if user_id in admins:
        return User(user_id)
    return None

# —— 辅助函数 —— #
def get_conn():
    return pymysql.connect(
        host='localhost', user='root', password='123456',
        database='flask', charset='utf8mb4', autocommit=True
    )

def get_client_ip():
    if request.headers.get('X-Forwarded-For'):
        return request.headers['X-Forwarded-For'].split(',')[0]
    return request.remote_addr

def get_ip_location(ip):
    try:
        res = requests.get(f"http://ip-api.com/json/{ip}?lang=zh-CN", timeout=3)
        data = res.json()
        if data.get('status') == 'success':
            return f"{data.get('country','')} {data.get('regionName','')} {data.get('city','')}"
    except:
        pass
    return "未知位置"

# —— 前台留言板 —— #
@app.route('/')
def index():
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT id, name, message, parent_id, created_at
                FROM messages
                WHERE status = 1
                ORDER BY created_at DESC
            """)
            messages = cursor.fetchall()
    except Exception as e:
        print("查询留言失败：", e)
        messages = []
    finally:
        conn.close()
    return render_template('index.html', messages=messages)

@app.route('/add_message', methods=['POST'])
def add_message():
    data = request.get_json()
    name = data.get('name','').strip()
    message = data.get('message','').strip()
    parent_id = data.get('parent_id') or None

    if not name or not message:
        return jsonify({'success': False, 'message': '姓名和留言不能为空'})

    ip = get_client_ip()
    ip_location = get_ip_location(ip)
    ua = request.headers.get('User-Agent','')
    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = get_conn()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO messages
                  (name, message, ip_address, ip_location, user_agent, created_at, status, parent_id)
                VALUES (%s,%s,%s,%s,%s,%s,1,%s)
            """, (name, message, ip, ip_location, ua, ct, parent_id))
        return jsonify({'success': True, 'message': '提交成功！'})
    except Exception as e:
        print("留言失败：", e)
        return jsonify({'success': False, 'message': '服务器错误，请稍后再试'})
    finally:
        conn.close()

# —— 管理员登录／注销 —— #
@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in admins and check_password_hash(admins[username], password):
            login_user(User(username))
            # 这里用 url_for 确保 endpoint 存在
            return redirect(url_for('admin'))
        flash('用户名或密码错误', 'error')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# —— 管理页面 —— #
@app.route('/admin')
@login_required
def admin():
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT id, name, message, parent_id, created_at, status
                FROM messages
                ORDER BY created_at DESC
            """)
            messages = cursor.fetchall()
    except Exception as e:
        print("查询留言失败：", e)
        messages = []
    finally:
        conn.close()
    return render_template('admin.html', messages=messages)

@app.route('/admin/delete_message', methods=['POST'])
@login_required
def delete_message():
    msg_id = request.get_json().get('id')
    if not msg_id:
        return jsonify({'success': False, 'message': '缺少留言ID'})
    try:
        conn = get_conn()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE messages SET status=0 WHERE id=%s", (msg_id,))
        return jsonify({'success': True, 'message': '删除成功'})
    except Exception as e:
        print("删除留言失败：", e)
        return jsonify({'success': False, 'message': '服务器错误，请稍后再试'})
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)
