# Server Connection Guide

## Server Info

| Item        | Value            |
|-------------|------------------|
| Host        | `116.62.157.253` |
| Username    | `root`           |
| OS          | Alibaba Cloud Linux 8 (AlmaLinux-based) |
| Kernel      | 5.10.134-19.5.al8.x86_64 |
| Disk        | 40G (4.9G used, 33G free) |

---

## 1. SSH Key (Recommended)

### Connect with Key

```bash
ssh -i /tmp/temp_key root@116.62.157.253
```

> **Note:** The private key is at `C:\Users\xinyi\AppData\Local\Temp\temp_key` on Windows (`/tmp/temp_key` in Git Bash).

### Make it Permanent (Optional)

Copy the key to `~/.ssh/` for persistent access:

```bash
# Git Bash
cp /tmp/temp_key ~/.ssh/alibaba-key
cp /tmp/temp_key.pub ~/.ssh/alibaba-key.pub
chmod 600 ~/.ssh/alibaba-key

# Set up config
echo -e "\nHost alibaba\n\tHostName 116.62.157.253\n\tUser root\n\tIdentityFile ~/.ssh/alibaba-key" >> ~/.ssh/config
```

Then connect with:
```bash
ssh alibaba
```

---

## 2. Password Login (Fallback)

```bash
ssh root@116.62.157.253
```

**Password:** `M@yJune1976`

> ⚠️ Password login may be disabled by the server. If it fails, use the SSH key method above.

---

## 3. Quick Commands

| Task | Command |
|------|---------|
| Connect | `ssh -i /tmp/temp_key root@116.62.157.253` |
| Run one command | `ssh -i /tmp/temp_key root@116.62.157.253 "command"` |
| Check disk | `df -h` |
| Check memory | `free -h` |
| Check uptime | `uptime` |
| View services | `systemctl list-units --type=service --state=running` |
| View logs | `journalctl -xe` |

---

## 4. Troubleshooting

**Permission denied on key**
```bash
# Re-upload the key (requires password)
ssh-copy-id -i /tmp/temp_key root@116.62.157.253
# or
python -c "
import paramiko, os
with open(os.path.join(os.environ['TEMP'], 'temp_key.pub')) as f:
    key = f.read().strip()
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('116.62.157.253', username='root', password='M@yJune1976')
ssh.exec_command('mkdir -p ~/.ssh && chmod 700 ~/.ssh')
i,o,e = ssh.exec_command('cat >> ~/.ssh/authorized_keys')
i.write(key + '\n'); i.flush(); i.channel.shutdown_write()
o.channel.recv_exit_status()
ssh.exec_command('chmod 600 ~/.ssh/authorized_keys')
ssh.close()
"
```

**`/c/Users/xinyi/.ssh/known_hosts: Permission denied`**
```bash
# Fix permissions on Windows
icacls "C:\Users\xinyi\.ssh\known_hosts" /reset
```

**Host key changed**
```bash
ssh-keygen -R 116.62.157.253
```
